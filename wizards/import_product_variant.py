""""Import product variant"""
# -*- coding: utf-8 -*-
#############################################################################
#
#    Cybrosys Technologies Pvt. Ltd.
#
#    Copyright (C) 2023-TODAY Cybrosys Technologies(<https://www.cybrosys.com>)
#    Author: Cybrosys Techno Solutions(<https://www.cybrosys.com>)
#
#    You can modify it under the terms of the GNU LESSER
#    GENERAL PUBLIC LICENSE (LGPL v3), Version 3.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU LESSER GENERAL PUBLIC LICENSE (LGPL v3) for more details.
#
#    You should have received a copy of the GNU LESSER GENERAL PUBLIC LICENSE
#    (LGPL v3) along with this program.
#    If not, see <http://www.gnu.org/licenses/>.
#
#############################################################################
import logging
import os
import base64, binascii, csv, io, tempfile, requests, xlrd
from odoo import fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare
from . import file_processors as fp
from . import product_operations as po
import itertools
import psycopg2
from odoo import api
from odoo.modules.registry import Registry

_logger = logging.getLogger(__name__)

class ImportVariant(models.TransientModel):
    """Wizard for selecting the imported Files"""
    _name = 'import.product.variant'
    _description = "Import Product Variants"

    import_file = fields.Selection(
        [('csv', 'CSV File'), ('excel', 'Excel File')], required=True,
        string="Import File", help="Import the files")
    method = fields.Selection([('create', 'Create Product'),
                             ('update', 'Update Product'),
                             ('update_product', 'Update Product Variant')],
                            string="Method", required=True,
                            help="Method for importing/Exporting")
    file = fields.Binary(string="File", required=True,
                        help="The file to upload")

    def action_import_product_variant(self):
        """This is used to import/export the product """
        if self.import_file == 'excel':
            rows = fp.process_excel_file(self.file)
            self._process_rows(rows)
        elif self.import_file == 'csv':
            rows, column_map = fp.process_csv_file(self.file)
            self._process_csv_rows(rows, column_map)
        return {'type': 'ir.actions.act_window_close'}

    def _process_rows(self, rows):
        """Process Excel rows."""
        # Since batching is required, collect rows into batches
        batch_size = 10  # Adjust as needed
        total_rows = len(rows)
        _logger.info(f"Total rows to process: {total_rows}")
        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_rows = rows[batch_start:batch_end]
            batch_number = batch_start // batch_size + 1
            _logger.info(f"Processing batch {batch_number}: Rows {batch_start+1} to {batch_end}")
            self._process_batch_rows(batch_rows)
            _logger.info(f"Completed processing batch {batch_number}")

    def _process_batch_rows(self, batch_rows):
        """Process a batch of Excel rows."""
        for row_vals in batch_rows:
            if len(row_vals) < int(24):
                raise UserError(_("Please ensure that you selected the correct file"))
            
            # Process category and units
            pro_category = po.process_category(self.env, row_vals[6])
            uom = po.process_uom(self.env, row_vals[7])
            po_uom = po.process_uom(self.env, row_vals[8])
            
            # Process taxes
            tax = po.process_tax(self.env, row_vals[9], 'sale')
            supplier_tax = po.process_tax(self.env, row_vals[10], 'purchase')
            
            # Get product type and invoice policy
            detailed = self._get_selection_key('detailed_type', row_vals[5])
            invoicing_type = self._get_selection_key('invoice_policy', row_vals[12])
            
            # Process image
            link = po.process_image(row_vals[23])
            
            # Create or update product
            self._create_or_update_product(
                row_vals, pro_category, uom, po_uom, tax, supplier_tax,
                detailed, invoicing_type, link
            )

    def _create_or_update_product(self, row_vals, pro_category, uom, po_uom, tax, supplier_tax, detailed, invoicing_type, link):
        """Create or update product based on the provided row values."""
        ProductTemplate = self.env['product.template']
        Product = self.env['product.product']

        # Prepare common values
        vals = {
            'default_code': row_vals[2],
            'name': row_vals[1],
            'sale_ok': row_vals[3],
            'purchase_ok': row_vals[4],
            'detailed_type': detailed,
            'categ_id': pro_category,
            'uom_id': uom,
            'uom_po_id': po_uom,
            'taxes_id': [(6, 0, [tax])] if tax else [(6, 0, [])],
            'supplier_taxes_id': [(6, 0, [supplier_tax])] if supplier_tax else [(6, 0, [])],
            'description_sale': row_vals[11],
            'invoice_policy': invoicing_type,
            'list_price': row_vals[13],
            'standard_price': row_vals[14],
            'weight': row_vals[19],
            'volume': row_vals[20],
        }
        if link:
            vals.update({'image_1920': link})

        # Check for barcode conflicts
        barcode = row_vals[18]
        if barcode:
            can_use_barcode, conflicting_product = self._check_barcode_conflicts(barcode)
            if not can_use_barcode:
                _logger.warning(
                    f"Barcode {barcode} already assigned to {conflicting_product.display_name}. "
                    f"Skipping barcode update for {row_vals[1]}"
                )
                vals.pop('barcode', None)  # Remove barcode from values to avoid conflict

        # Determine if product exists
        product = False
        if row_vals[18]:  # Search by barcode
            product = ProductTemplate.search([('barcode', '=', row_vals[18])], limit=1)
        if not product and row_vals[2]:  # Search by internal reference
            product = ProductTemplate.search([('default_code', '=', row_vals[2])], limit=1)

        # Ensure barcode conflict check happens before any write/create
        if product:
            barcode = row_vals[18]
            if barcode:
                can_use_barcode, conflicting_product = self._check_barcode_conflicts(barcode, product)
                if not can_use_barcode:
                    _logger.warning(
                        f"Barcode {barcode} already assigned to {conflicting_product.display_name}. "
                        f"Skipping barcode update for {row_vals[1]}"
                    )
                    vals.pop('barcode', None)  # Remove barcode from values to avoid conflict

        # Proceed with product creation or update only after conflict check
        if product:
            # Update existing product
            product.write(vals)
        else:
            # Create new product
            product = ProductTemplate.create(vals)

        # Generate external ID for the product variant
        variant_external_id = f"product_product_{row_vals[2].replace(' ', '_').lower()}"
        _logger.info(f"Setting Variant External ID: {variant_external_id}")
        # Check for existing variant with external ID
        existing_variant_external_id = self.env['ir.model.data'].search([
            ('model', '=', 'product.product'),
            ('res_id', '=', product.id)
        ], limit=1)

        if existing_variant_external_id:
            _logger.info(f"Using existing external ID for variant: {existing_variant_external_id.name}")
        else:
            # Create in a separate transaction
            with Registry(self.env.cr.dbname).cursor() as new_cr:
                try:
                    new_env = api.Environment(new_cr, self.env.uid, self.env.context)
                    new_env['ir.model.data'].create({
                        'name': variant_external_id,
                        'model': 'product.product',
                        'res_id': product.id,
                        'module': '__import__'
                    })
                    new_cr.commit()
                    _logger.info(f"Created external ID for variant: {variant_external_id}")
                except Exception as e:
                    _logger.warning(f"Could not create external ID for variant {product.display_name}: {str(e)}")
        # Handle quantity updates through inventory adjustment
        qty_value = row_vals[21]  # Assuming quantity is in column 21
        if qty_value.strip() if isinstance(qty_value, str) else qty_value:
            qty = float(qty_value or '0.0')
            location = self.env['stock.location'].search([
                ('usage', '=', 'internal'),
                ('company_id', '=', self.env.company.id)
            ], limit=1)

            if location:
                current_qty = product.with_context(location=location.id).qty_available if location else 0.0
                if float_compare(qty, current_qty, precision_digits=2) != 0:
                    inventory = self.env['stock.inventory'].create({
                        'name': f'Import adjustment for {product.display_name}',
                        'product_ids': [(4, product.id)],
                        'location_ids': [(4, location.id)],
                        'start_empty': True,
                    })
                    _logger.info(f"Creating inventory adjustment for variant {product.display_name} with qty {qty} at location {location.display_name}")
                    try:
                        # Log input values for inventory creation
                        _logger.debug(f"Inventory creation inputs: name='Import adjustment for {product.display_name}', product_ids=[(4, {product.id})], location_ids=[(4, {location.id})], start_empty=True")
                        
                        _logger.info(f"Created inventory adjustment {inventory.name} (ID: {inventory.id})")
                        
                        # Start inventory
                        inventory.action_start()
                        _logger.info(f"Started inventory adjustment {inventory.name}")
                        
                        # Log input values for inventory line creation
                        _logger.debug(f"Inventory line creation inputs: inventory_id={inventory.id}, product_id={product.id}, location_id={location.id}, product_qty={qty}")
                        
                        # Create inventory line
                        line = self.env['stock.inventory.line'].create({
                            'inventory_id': inventory.id,
                            'product_id': product.id,
                            'location_id': location.id,
                            'product_qty': qty,
                        })
                        _logger.info(f"Created inventory line for {product.display_name}")
                        
                        # Validate inventory
                        inventory.action_validate()
                        _logger.info(f"Validated inventory adjustment {inventory.name}")
                    except Exception as e:
                        _logger.error(f"Error during inventory adjustment for variant {product.display_name}: {str(e)}", exc_info=True)
            else:
                _logger.error("No internal location found for the current company. Inventory adjustment cannot be created.")
        else:
            _logger.info(f"Skipping quantity update for variant {product.display_name} (no quantity specified in import)")

    def _check_barcode_conflicts(self, barcode, product=False):
        """Check if a barcode is already assigned to another product."""
        if not barcode:
            return True, False

        domain = [('barcode', '=', barcode)]
        if product:
            domain.append(('id', '!=', product.id))
            
        existing_product = self.env['product.product'].search(domain, limit=1)
        if not existing_product:
            # Check product templates
            domain = [('barcode', '=', barcode)]
            if product and product.product_tmpl_id:
                domain.append(('id', '!=', product.product_tmpl_id.id))
            existing_template = self.env['product.template'].search(domain, limit=1)
            if existing_template:
                return False, existing_template
                
        return not bool(existing_product), existing_product

    def _process_csv_rows(self, rows, column_map):
        """Process CSV rows."""
        # Validate required columns
        required_columns = ['Name', 'Category']
        missing_columns = [col for col in required_columns if col not in column_map]
        if missing_columns:
            raise UserError(_("Missing required columns: %s") % ", ".join(missing_columns))
        
        # Collect rows into batches based on product templates
        batch_size = 50  # Adjust as needed
        total_rows = len(rows)
        _logger.info(f"Total rows to process: {total_rows}")
        # Collect all rows first
        product_data_list = []
        for row_num, row in enumerate(rows, start=2):
            valid, error_msg = fp.validate_row_data(row, len(column_map), row_num, required_columns)
            if not valid:
                _logger.warning(error_msg)
                continue
            
            values = {col: fp.process_cell_value(row[idx]) for col, idx in column_map.items() if idx < len(row)}
            product_data_list.append(values)
        
        # Group product data by product templates
        products_map = {}  # group_key -> [values]
        for values in product_data_list:
            group_key = values.get('Unique Identifier') or values.get('Name')
            if not group_key:
                _logger.warning(f"Skipping row: No Unique Identifier or Name found")
                continue
            if group_key not in products_map:
                products_map[group_key] = []
            products_map[group_key].append(values)
        
        # Now process products in batches
        product_keys = list(products_map.keys())
        total_products = len(product_keys)
        _logger.info(f"Total products to process: {total_products}")
        for batch_start in range(0, total_products, batch_size):
            batch_end = min(batch_start + batch_size, total_products)
            batch_product_keys = product_keys[batch_start:batch_end]
            batch_number = batch_start // batch_size + 1
            _logger.info(f"Processing batch {batch_number}: Products {batch_start+1} to {batch_end}")
            for group_key in batch_product_keys:
                product_values_list = products_map[group_key]
                self._process_product_template(group_key, product_values_list)
            _logger.info(f"Completed processing batch {batch_number}")
        _logger.info("Finished processing all batches")

    def _process_product_template(self, group_key, product_values_list):
        """Process a single product template and its variants following the defined flow:
        1. Check if we need to skip records based on method
        2. Search for template by external ID, internal reference, or barcode
        3. Create template if it doesn't exist
        4. Create template variants
        5. Store database IDs for later use
        6. Create external IDs for template and variants
        """
        _logger.info(f"=== Processing template group: {group_key} ===")
        _logger.info(f"Number of variants to process: {len(product_values_list)}")
        
        # Step 1: Extract template values
        template_values = product_values_list[0].copy()
        template_ref = template_values.get('Template Internal Reference') or template_values.get('Internal Reference')
        
        if not template_ref:
            _logger.error("No template reference found in values")
            return False
            
        # Step 2: Find or create template with proper locking
        self.env.cr.execute("""
            SELECT id FROM product_template 
            WHERE default_code = %s
            FOR UPDATE NOWAIT
        """, (template_ref,))
        
        product_tmpl = self._find_existing_template(template_values)
        if not product_tmpl:
            _logger.info(f"Creating new template with reference: {template_ref}")
            product_tmpl = self._create_product_template(template_values)
            if not product_tmpl:
                return False
        
        # Verify template reference matches
        if product_tmpl.default_code != template_ref:
            _logger.error(f"Template reference mismatch. Expected: {template_ref}, Found: {product_tmpl.default_code}")
            return False
        
        # Step 3: Process variants with additional verification
        processed_variants = []
        for values in product_values_list:
            # Verify variant belongs to this template
            variant_template_ref = values.get('Template Internal Reference') or values.get('Internal Reference')
            if variant_template_ref != template_ref:
                _logger.error(f"Variant template reference mismatch. Expected: {template_ref}, Found: {variant_template_ref}")
                continue
                
            variant = self._create_or_update_variant(product_tmpl, values)
            if variant:
                processed_variants.append(variant)
        
        _logger.info(f"=== Completed template processing. Processed {len(processed_variants)} variants ===")
        return processed_variants

    def _find_existing_template(self, template_values):
        """Search for existing template using multiple criteria"""
        # Resolve the unique reference value, allowing for different CSV key names.
        template_ref = template_values.get('Internal Reference') or template_values.get('Template Internal Reference')
        if template_ref:
            existing = self.env['product.template'].search([
                ('default_code', '=', template_ref)
            ], limit=1)
            if existing:
                return existing

        # Priority 2: Check by external ID
        if template_values.get('External ID'):
            existing = self.env.ref(template_values['External ID'], raise_if_not_found=False)
            if existing:
                return existing

        # Priority 3: Check by barcode, with validation (if a reference value exists)
        if template_values.get('Barcode'):
            barcode = template_values.get('Barcode').strip()
            existing = self.env['product.template'].search([
                ('barcode', '=', barcode)
            ], limit=1)
            if existing:
                if template_ref and existing.default_code != template_ref:
                    _logger.error("Template reference mismatch. Expected: %s, Found: %s", template_ref, existing.default_code)
                    raise UserError(_("Template reference mismatch detected. Please reconcile identifiers."))
                return existing

        return self.env['product.template']

    def _create_product_template(self, template_values):
        """Create a new product template"""
        vals = self._prepare_template_values(template_values)
        return self.env['product.template'].create(vals)

    def _process_variants(self, product_tmpl, product_values_list):
        """Process variants for a product template"""
        processed_variants = []
        
        # First, prepare attribute lines
        self._prepare_attribute_lines(product_tmpl, product_values_list)
        
        # Process each variant in the import data
        for values in product_values_list:
            variant = self._create_or_update_variant(product_tmpl, values)
            if variant:
                processed_variants.append({
                    'variant': variant,
                    'values': values
                })
        
        return processed_variants

    def _find_variant_by_combination(self, product_tmpl, values):
        """Find variant by its attribute combination"""
        if not (values.get('Variant Attributes') and values.get('Attribute Values')):
            return False
        
        attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
        attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
        
        if len(attribute_names) != len(attribute_values):
            _logger.warning(f"Mismatch in attribute counts for {product_tmpl.name}")
            return False

        # Lock the product template to prevent concurrent variant creation
        self.env.cr.execute("""
            SELECT id FROM product_template 
            WHERE id = %s 
            FOR UPDATE NOWAIT
        """, (product_tmpl.id,))
        
        # Get all product variants with a fresh query
        variants = self.env['product.product'].search([
            ('product_tmpl_id', '=', product_tmpl.id)
        ])
        
        # Create a set of attribute value combinations for faster lookup
        variant_combinations = {}
        for variant in variants:
            key_parts = []
            for attr_name in attribute_names:
                attr_value = variant.product_template_attribute_value_ids.filtered(
                    lambda x: x.attribute_id.name == attr_name
                ).product_attribute_value_id.name
                if attr_value:
                    key_parts.append((attr_name, attr_value))
            if key_parts:
                variant_combinations[tuple(sorted(key_parts))] = variant

        # Create key for the current combination
        current_combination = tuple(sorted(zip(attribute_names, attribute_values)))
        
        # Look for exact match
        matching_variant = variant_combinations.get(current_combination)
        if matching_variant:
            _logger.info(f"Found matching variant for combination: {values['Attribute Values']}")
            return matching_variant
            
        _logger.info(f"No matching variant found for combination: {values['Attribute Values']}")
        return False

    def _create_or_update_variant(self, product_tmpl, values):
        """Create or update a product variant"""
        _logger.info(f"Creating or updating variant for {product_tmpl.name}")
        
        # Lock at transaction level to prevent concurrent variant creation
        self.env.cr.execute("SELECT id FROM product_template WHERE id = %s FOR UPDATE", (product_tmpl.id,))
        
        # Refresh the product template from database to ensure we have latest data
        product_tmpl.flush_recordset()  # Flush any pending changes
        product_tmpl.invalidate_recordset()  # Invalidate cache in Odoo 17
        product_tmpl = self.env['product.template'].browse(product_tmpl.id)
        
        # First try to find the variant by combination
        variant = self._find_variant_by_combination(product_tmpl, values)
        
        if not variant and values.get('Internal Reference'):
            variant = self._find_variant_by_default_code(product_tmpl, values)
        
        if not variant and values.get('Barcode'):
            variant = self.env['product.product'].search([
                ('barcode', '=', values['Barcode']),
                ('product_tmpl_id', '=', product_tmpl.id)
            ], limit=1)

        # If no variant found and we have attribute values, try to create one
        if not variant and values.get('Variant Attributes') and values.get('Attribute Values'):
            try:
                attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
                attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
                
                if len(attribute_names) != len(attribute_values):
                    _logger.warning(f"Mismatch in attribute counts for {product_tmpl.name}")
                    return False

                # Get all product template attribute lines with a fresh query
                template_attribute_lines = self.env['product.template.attribute.line'].search([
                    ('product_tmpl_id', '=', product_tmpl.id)
                ])
                
                # Build the attribute value combination
                value_combination = []
                for attr_name, attr_value in zip(attribute_names, attribute_values):
                    attr_line = template_attribute_lines.filtered(
                        lambda l: l.attribute_id.name == attr_name
                    )
                    if not attr_line:
                        _logger.warning(f"Attribute {attr_name} not found in template {product_tmpl.name}")
                        continue

                    attr_value_id = attr_line.value_ids.filtered(
                        lambda v: v.name == attr_value
                    )
                    if not attr_value_id:
                        _logger.warning(f"Value {attr_value} not found for attribute {attr_name}")
                        continue

                    ptav = self.env['product.template.attribute.value'].search([
                        ('product_tmpl_id', '=', product_tmpl.id),
                        ('product_attribute_value_id', '=', attr_value_id.id)
                    ], limit=1)
                    
                    if ptav:
                        value_combination.append(ptav.id)

                if value_combination:
                    # Ensure we have the latest state before creating variant
                    self.env.cr.commit()  # Commit current transaction
                    
                    # Use Odoo's native variant creation mechanism in a new transaction
                    variant = product_tmpl.with_context(create_product_product=True)._create_product_variant(
                        product_template_attribute_value_ids=value_combination
                    )
                    
                    if variant:
                        _logger.info(f"Created new variant for {product_tmpl.name}")
                    else:
                        _logger.warning(f"Failed to create variant for {product_tmpl.name}")
                        return False

            except Exception as e:
                _logger.error(f"Failed to create variant: {str(e)}")
                return False

        if not variant:
            _logger.warning(f"Could not find or create variant for {product_tmpl.name}")
            return False

        # Update variant values
        update_vals = {}
        
        # Handle internal reference
        internal_ref = values.get('Internal Reference', '').strip()
        if internal_ref:
            existing_product = self.env['product.product'].search([
                ('default_code', '=', internal_ref),
                ('id', '!=', variant.id)
            ], limit=1)
            if not existing_product:
                update_vals['default_code'] = internal_ref
                _logger.info(f"Setting internal reference {internal_ref} for variant")
        
        # Handle barcode
        barcode = values.get('Barcode', '').strip()
        if barcode:
            existing_product = self.env['product.product'].search([
                ('barcode', '=', barcode),
                ('id', '!=', variant.id)
            ], limit=1)
            if not existing_product:
                update_vals['barcode'] = barcode
                _logger.info(f"Setting barcode {barcode} for variant")
        
        # Handle cost
        if values.get('Cost'):
            try:
                cost_value = float(values['Cost'])
                update_vals['standard_price'] = cost_value
                _logger.info(f"Setting cost price to {cost_value} for variant with combination {values.get('Variant Attributes')}")
            except (ValueError, TypeError):
                _logger.warning(f"Invalid cost value: {values['Cost']}")
        
        # Handle quantity from either column name
        qty_value = values.get('Qty On Hand', values.get('Quantity', ''))
        if qty_value.strip() if isinstance(qty_value, str) else qty_value:
            vals['qty_available'] = float(qty_value or '0.0')

        # Handle attribute values
        if values.get('Variant Attributes') and values.get('Attribute Values'):
            attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
            attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
            
            if len(attribute_names) != len(attribute_values):
                _logger.warning(f"Mismatch in attribute counts for {product_tmpl.name}")
                return False

            # Get all product template attribute lines
            template_attribute_lines = product_tmpl.attribute_line_ids

            # Collect attribute value IDs in the order they appear in the template
            attribute_value_ids = []
            for attr_name, attr_value in zip(attribute_names, attribute_values):
                # Find the attribute line for this attribute
                attr_line = template_attribute_lines.filtered(
                    lambda l: l.attribute_id.name == attr_name
                )
                if not attr_line:
                    _logger.warning(f"Attribute {attr_name} not found in template {product_tmpl.name}")
                    continue

                # Find the attribute value
                attr_value_id = attr_line.value_ids.filtered(
                    lambda v: v.name == attr_value
                )
                if not attr_value_id:
                    _logger.warning(f"Value {attr_value} not found for attribute {attr_name}")
                    continue

                # Get the product template attribute value
                ptav = self.env['product.template.attribute.value'].search([
                    ('product_tmpl_id', '=', product_tmpl.id),
                    ('product_attribute_value_id', '=', attr_value_id.id)
                ], limit=1)
                
                if ptav:
                    attribute_value_ids.append(ptav.id)

            if attribute_value_ids:
                update_vals['product_template_attribute_value_ids'] = [(6, 0, attribute_value_ids)]
                _logger.info(f"Setting attribute values for variant: {attribute_value_ids}")

        if update_vals:
            try:
                variant.write(update_vals)
                _logger.info(f"Successfully updated variant with values: {update_vals}")
            except Exception as e:
                _logger.error(f"Failed to update variant values: {str(e)}")
                
        # Ensure external IDs are created
        self._create_template_external_ids(variant.product_tmpl_id, values)
        self._create_variant_external_ids(variant, values)
        return variant

    def _update_variant_identifiers(self, variant, values):
        """Update variant identifiers and cost"""
        update_vals = {}
        
        # Handle internal reference
        internal_ref = values.get('Internal Reference', '').strip()
        if internal_ref:
            existing_product = self.env['product.product'].search([
                ('default_code', '=', internal_ref),
                ('id', '!=', variant.id)
            ], limit=1)
            if not existing_product:
                update_vals['default_code'] = internal_ref
                _logger.info(f"Setting internal reference {internal_ref} for variant")
        
        # Handle barcode
        barcode = values.get('Barcode', '').strip()
        if barcode:
            existing_product = self.env['product.product'].search([
                ('barcode', '=', barcode),
                ('id', '!=', variant.id)
            ], limit=1)
            if not existing_product:
                update_vals['barcode'] = barcode
                _logger.info(f"Setting barcode {barcode} for variant")
        
        # Handle cost
        if values.get('Cost'):
            try:
                cost_value = float(values['Cost'])
                update_vals['standard_price'] = cost_value
                _logger.info(f"Setting cost price to {cost_value} for variant with combination {values.get('Variant Attributes')}")
            except (ValueError, TypeError):
                _logger.warning(f"Invalid cost value: {values['Cost']}")
        
        if update_vals:
            try:
                variant.write(update_vals)
                _logger.info(f"Successfully updated variant with values: {update_vals}")
            except Exception as e:
                _logger.error(f"Failed to update variant values: {str(e)}")
                
        # Ensure external IDs are created
        self._create_template_external_ids(variant.product_tmpl_id, values)
        self._create_variant_external_ids(variant, values)
        return variant

    def _create_template_external_ids(self, product_tmpl, template_values):
        """Create external IDs for template"""
        template_ref = template_values.get('Template Internal Reference') or template_values.get('Internal Reference')
        if template_ref:
            external_id = f"product_tmpl_{template_ref.replace(' ', '_').lower()}"
            self._create_external_id(product_tmpl, external_id)

    def _create_variant_external_ids(self, variant, values):
        """Create external IDs for variant"""
        variant_ref = values.get('Internal Reference', '').strip()
        if variant_ref:
            external_id = f"product_product_{variant_ref.replace(' ', '_').lower()}"
            self._create_external_id(variant, external_id)

    def _create_external_id(self, record, external_id):
        """Create external ID for a record"""
        if not self.env['ir.model.data'].search([
            ('model', '=', record._name),
            ('res_id', '=', record.id)
        ]):
            self.env['ir.model.data'].create({
                'name': external_id,
                'model': record._name,
                'res_id': record.id,
                'module': '__import__'
            })

    def _prepare_template_values(self, template_values):
        """Prepare values for creating a new product template"""
        vals = {
            'name': template_values.get('Name', ''),
            'default_code': template_values.get('Internal Reference', ''),
            'sale_ok': template_values.get('Can be sold', 'True').lower() == 'true',
            'purchase_ok': template_values.get('Can be Purchased', 'True').lower() == 'true',
            'detailed_type': 'product',
            'description_sale': template_values.get('Description for customers', ''),
            'list_price': float(template_values.get('Sales Price', '0.0') or '0.0'),
            'standard_price': float(template_values.get('Cost', '0.0') or '0.0'),
            'weight': float(template_values.get('Weight', '0.0') or '0.0'),
            'volume': float(template_values.get('Volume', '0.0') or '0.0'),
            'available_in_pos': template_values.get('Available in POS', 'True').lower() == 'true',
        }

        # Handle barcode separately to avoid constraint errors
        barcode = template_values.get('Barcode', '').strip()
        if barcode:
            if self.method in ['update', 'update_product']:
                # In update modes, first check if there's a product with this barcode
                existing_product = self.env['product.product'].search([('barcode', '=', barcode)], limit=1)
                if existing_product:
                    _logger.info(f"Found existing variant. ID: {existing_product.id} with template ID: {existing_product.product_tmpl_id.id}")
                    # Use the parent template of the found variant
                    product_tmpl = existing_product.product_tmpl_id
                    _logger.info(f"Using parent product template found by barcode: {barcode}")
                else:
                    _logger.info(f"No existing product found for barcode: {barcode}")
                vals['barcode'] = barcode
            else:
                # In create mode, only set barcode if it's not used
                if not self.env['product.product'].search_count([('barcode', '=', barcode)]):
                    vals['barcode'] = barcode
                else:
                    _logger.warning(f"Skipping duplicate barcode in create mode: {barcode}")

        # Process image if provided
        if template_values.get('Image'):
            image_path = template_values.get('Image')
            image_data = po.process_image(image_path)
            if image_data:
                vals['image_1920'] = image_data

        if template_values.get('POS Category'):
            pos_category = template_values['POS Category'].strip()
            if pos_category:
                categories = pos_category.split('/')
                parent_id = None
                pos_category_ids = []
                for cat in categories:
                    if cat.strip():
                        domain = [('name', '=', cat.strip())]
                        if parent_id:
                            domain.append(('parent_id', '=', parent_id))
                        pos_categ = self.env['pos.category'].search(domain, limit=1)
                        if not pos_categ:
                            pos_categ = self.env['pos.category'].create({
                                'name': cat.strip(),
                                'parent_id': parent_id
                            })
                        parent_id = pos_categ.id
                        pos_category_ids.append(pos_categ.id)
                if pos_category_ids:
                    vals['pos_categ_ids'] = [(6, 0, pos_category_ids)]

        # Process category
        category = template_values.get('Category', '').strip()
        if category:
            categories = category.split('/')
            parent_id = None
            for cat in categories:
                if cat.strip():
                    domain = [('name', '=', cat.strip())]
                    if parent_id:
                        domain.append(('parent_id', '=', parent_id))
                    category_obj = self.env['product.category'].search(domain, limit=1)
                    if not category_obj:
                        category_obj = self.env['product.category'].create({
                            'name': cat.strip(),
                            'parent_id': parent_id
                        })
                    parent_id = category_obj.id
            if parent_id:
                vals['categ_id'] = parent_id

        # Process UoM
        if template_values.get('Unit of Measure'):
            uom = self.env['uom.uom'].search([('name', 'ilike', template_values['Unit of Measure'])], limit=1)
            if not uom:
                raise UserError(_("Unit of Measure '%s' not found") % template_values['Unit of Measure'])
            vals['uom_id'] = uom.id
            vals['uom_po_id'] = uom.id  # Set purchase UoM same as default if not specified

        if template_values.get('Purchase Unit of Measure'):
            po_uom = self.env['uom.uom'].search([('name', 'ilike', template_values['Purchase Unit of Measure'])], limit=1)
            if not po_uom:
                raise UserError(_("Purchase Unit of Measure '%s' not found") % template_values['Purchase Unit of Measure'])
            vals['uom_po_id'] = po_uom.id

        # Process taxes
        if template_values.get('Customer Taxes'):
            tax_id = po.process_tax(self.env, template_values['Customer Taxes'], 'sale')
            if tax_id:
                vals['taxes_id'] = [(6, 0, [tax_id])]

        if template_values.get('Vendor Taxes'):
            supplier_tax_id = po.process_tax(self.env, template_values['Vendor Taxes'], 'purchase')
            if supplier_tax_id:
                vals['supplier_taxes_id'] = [(6, 0, [supplier_tax_id])]

        return vals

    def _prepare_variant_values(self, product_tmpl, values):
        """Prepare values for creating a new product variant"""
        vals = {
            'product_tmpl_id': product_tmpl.id,
        }

        # Always include internal reference if available
        internal_ref = values.get('Internal Reference', '').strip()
        if internal_ref:
            vals['default_code'] = internal_ref
        elif values.get('Default Code', '').strip():
            vals['default_code'] = values.get('Default Code').strip()

        # Always include barcode if available
        barcode = values.get('Barcode', '').strip()
        if barcode:
            vals['barcode'] = barcode

        # Handle cost price
        cost = values.get('Cost', '').strip()
        if cost:
            try:
                vals['standard_price'] = float(cost)
                _logger.info(f"Setting cost price to {cost} for variant with combination {values.get('Variant Attributes')}")
            except (ValueError, TypeError) as e:
                _logger.warning(f"Invalid cost value '{cost}': {str(e)}")

        # Handle quantity from either column name
        qty_value = values.get('Qty On Hand', values.get('Quantity', ''))
        if qty_value.strip() if isinstance(qty_value, str) else qty_value:
            vals['qty_available'] = float(qty_value or '0.0')

        # Handle attribute values
        if values.get('Variant Attributes') and values.get('Attribute Values'):
            attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
            attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
            
            if len(attribute_names) != len(attribute_values):
                _logger.warning(f"Mismatch in attribute counts for {product_tmpl.name}")
                return False

            # Get all product template attribute lines
            template_attribute_lines = product_tmpl.attribute_line_ids

            # Collect attribute value IDs in the order they appear in the template
            attribute_value_ids = []
            for attr_name, attr_value in zip(attribute_names, attribute_values):
                # Find the attribute line for this attribute
                attr_line = template_attribute_lines.filtered(
                    lambda l: l.attribute_id.name == attr_name
                )
                if not attr_line:
                    _logger.warning(f"Attribute {attr_name} not found in template {product_tmpl.name}")
                    continue

                # Find the attribute value
                attr_value_id = attr_line.value_ids.filtered(
                    lambda v: v.name == attr_value
                )
                if not attr_value_id:
                    _logger.warning(f"Value {attr_value} not found for attribute {attr_name}")
                    continue

                # Get the product template attribute value
                ptav = self.env['product.template.attribute.value'].search([
                    ('product_tmpl_id', '=', product_tmpl.id),
                    ('product_attribute_value_id', '=', attr_value_id.id)
                ], limit=1)
                
                if ptav:
                    attribute_value_ids.append(ptav.id)

            if attribute_value_ids:
                vals['product_template_attribute_value_ids'] = [(6, 0, attribute_value_ids)]
                _logger.info(f"Setting attribute values for variant: {attribute_value_ids}")

        return vals

    def _find_variant_by_default_code(self, product_tmpl, values):
        """Find existing variant by default_code"""
        Product = self.env['product.product']
        
        # Try to find existing variant
        variant = Product.search([
            ('product_tmpl_id', '=', product_tmpl.id),
            ('default_code', '=', values.get('Internal Reference') or values.get('Default Code'))
        ], limit=1)
        
        return variant

    def _get_selection_key(self, field_name, value):
        """Get selection key from field selection."""
        field_selection = dict(self.env['product.template']._fields[field_name].selection)
        for key, val in field_selection.items():
            if val == value:
                return key
        return None

    def process_product(self, values):
        """Process a single product from the import data"""
        if not values.get('Internal Reference'):
            _logger.warning("Skipping row without Internal Reference")
            return False
            
        product_tmpl = self._find_or_create_template(values)
        if not product_tmpl:
            return False
            
        # Always try to create/update variant
        variant = self._create_or_update_variant(product_tmpl, values)
        
        # Double check that variant has all required fields
        if variant and not (variant.default_code and variant.barcode):
            _logger.warning(f"Variant {variant.id} missing required fields, forcing update")
            self._update_variant_identifiers(variant, values)
            
        return variant

    def _find_or_create_template(self, values):
        """Find or create a product template"""
        ProductTemplate = self.env['product.template']
        
        # Try to find existing template
        template_ref = values.get('Template Internal Reference') or values.get('Internal Reference')
        if template_ref:
            template = ProductTemplate.search([('default_code', '=', template_ref)], limit=1)
            if template:
                return template
        
        # Try to find by barcode
        barcode = values.get('Barcode', '').strip()
        if barcode:
            template = ProductTemplate.search([('barcode', '=', barcode)], limit=1)
            if template:
                return template
        
        # If no template found, create one
        return self._create_product_template(values)

    def _prepare_attribute_lines(self, product_tmpl, product_values_list):
        """Prepare attribute lines for the template"""
        ProductAttribute = self.env['product.attribute']
        ProductAttributeValue = self.env['product.attribute.value']
        ProductTemplateAttributeLine = self.env['product.template.attribute.line']
        
        # Get existing attribute lines
        existing_attr_lines = ProductTemplateAttributeLine.search([
            ('product_tmpl_id', '=', product_tmpl.id)
        ])
        
        # Collect all attributes and values
        attribute_value_mapping = {}
        for values in product_values_list:
            if values.get('Variant Attributes') and values.get('Attribute Values'):
                attributes = values['Variant Attributes'].split(',')
                values_list = values['Attribute Values'].split(';')
                
                if len(attributes) != len(values_list):
                    raise UserError(_(
                        "Number of attributes ({}) does not match number of values ({}) for product '{}'"
                    ).format(len(attributes), len(values_list), values.get('Name', '')))
                
                for attr_name, attr_value in zip(attributes, values_list):
                    attr_name = attr_name.strip()
                    attr_value = attr_value.strip()
                    if attr_name not in attribute_value_mapping:
                        attribute_value_mapping[attr_name] = set()
                    attribute_value_mapping[attr_name].add(attr_value)
        
        # Process each attribute
        for attr_name, attr_values in attribute_value_mapping.items():
            # Find or create attribute
            attribute = ProductAttribute.search([('name', '=', attr_name)], limit=1)
            if not attribute:
                attribute = ProductAttribute.create({
                    'name': attr_name,
                    'create_variant': 'always'
                })
            
            # Find or create attribute values
            value_ids = []
            for attr_value in attr_values:
                value = ProductAttributeValue.search([
                    ('name', '=', attr_value),
                    ('attribute_id', '=', attribute.id)
                ], limit=1)
                if not value:
                    value = ProductAttributeValue.create({
                        'name': attr_value,
                        'attribute_id': attribute.id
                    })
                value_ids.append(value.id)
            
            # Find existing attribute line
            attr_line = existing_attr_lines.filtered(
                lambda l: l.attribute_id.id == attribute.id
            )
            
            if attr_line:
                # Update existing line with new values
                current_values = attr_line.value_ids.ids
                new_values = list(set(current_values + value_ids))
                if new_values != current_values:
                    try:
                        attr_line.write({'value_ids': [(6, 0, new_values)]})
                    except Exception as e:
                        _logger.warning(
                            f"Failed to update attribute line for {attr_name}: {str(e)}"
                        )
            else:
                # Create new attribute line
                try:
                    ProductTemplateAttributeLine.create({
                        'product_tmpl_id': product_tmpl.id,
                        'attribute_id': attribute.id,
                        'value_ids': [(6, 0, value_ids)]
                    })
                except Exception as e:
                    _logger.warning(
                        f"Failed to create attribute line for {attr_name}: {str(e)}"
                    )
        
        return True