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
import base64
import csv
import io
import logging
import requests
from datetime import datetime
import binascii, tempfile, xlrd
from odoo import fields, models, _
from odoo.exceptions import UserError
from odoo.tools import float_compare
from . import file_processors as fp
from . import product_operations as po
import itertools
import psycopg2
from odoo import api
from odoo.modules.registry import Registry
import re

_logger = logging.getLogger(__name__)

class ImportVariant(models.TransientModel):
    """Wizard for selecting the imported Files"""
    _name = 'import.product.variant'
    _description = "Import Product Variants"

    import_file = fields.Selection(
        [('csv', 'CSV File'), ('excel', 'Excel File')], required=True,
        string="Import File", help="Import the file")
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
        """Process data rows from the CSV file."""
        _logger.info("Starting to process CSV rows")
        
        # Validate required columns
        required_columns = ['Name', 'Category']
        missing_columns = [col for col in required_columns if col not in column_map]
        if missing_columns:
            raise UserError(_("Missing required columns: %s") % ", ".join(missing_columns))
        
        # Pre-process templates to avoid duplicates
        template_references = set()
        for row in rows:
            template_ref_index = column_map.get('Template Internal Reference') or column_map.get('Internal Reference')
            if template_ref_index is not None and template_ref_index < len(row):
                template_ref = row[template_ref_index]
                template_references.add(template_ref)
        
        _logger.info(f"Total unique template references: {len(template_references)}")
        
        # Find existing templates
        existing_templates = {}
        ProductTemplate = self.env['product.template']
        for template_ref in template_references:
            template = ProductTemplate.search([('default_code', '=', template_ref)], limit=1)
            if template:
                _logger.info(f"Found existing template for reference: {template_ref}. ID: {template.id}, Name: {template.name}")
                existing_templates[template_ref] = template
        
        _logger.info(f"Found {len(existing_templates)} existing templates")
        
        # Create missing templates
        new_templates = {}
        for template_ref in template_references:
            if template_ref not in existing_templates:
                # Find the product_values_list for this template_ref
                product_values_list = []
                for row in rows:
                    template_ref_index = column_map.get('Template Internal Reference') or column_map.get('Internal Reference')
                    if template_ref_index is not None and template_ref_index < len(row):
                        if row[template_ref_index] == template_ref:
                            product_values_list.append(row)
        
                if product_values_list:
                    # Create a dictionary from the list for _create_product_template
                    template_values = {}
                    for col, index in column_map.items():
                        if index < len(product_values_list[0]):
                            template_values[col] = product_values_list[0][index]
                    template = self._create_product_template(template_values)
                    if template:
                        new_templates[template_ref] = template
                        _logger.info(f"Created new template for reference: {template_ref}. ID: {template.id}, Name: {template.name}")
                    else:
                        _logger.error(f"Failed to create template for reference: {template_ref}")
                else:
                    _logger.warning(f"No product values found for template reference: {template_ref}")
        
        _logger.info(f"Created {len(new_templates)} new templates")
        
        # Combine existing and new templates
        all_templates = existing_templates.copy()
        all_templates.update(new_templates)
        
        _logger.info(f"Total templates (existing + new): {len(all_templates)}")
        
        # Process product data in batches
        # Collect rows into batches based on product templates
        products = {}
        for row in rows:
            template_ref_index = column_map.get('Template Internal Reference') or column_map.get('Internal Reference')
            if template_ref_index is not None and template_ref_index < len(row):
                template_ref = row[template_ref_index]
            else:
                template_ref = 'No Template' # Group products without template
            if template_ref not in products:
                products[template_ref] = []
            products[template_ref].append(row)
        
        total_products = len(products)
        
        _logger.info(f"Total products to process: {total_products}")
        
        batch_size = 50
        product_count = 0
        for i, group_key in enumerate(products):
            product_values_list = products[group_key]
            start = i * batch_size + 1
            end = min((i + 1) * batch_size, total_products)
            _logger.info(f"Processing batch {i + 1}: Products {start} to {end}")
        
            # Retrieve the template from the dictionary
            if group_key != 'No Template':
                product_tmpl = all_templates.get(group_key)
                if not product_tmpl:
                    _logger.error(f"No template found for reference: {group_key}")
                    continue
            else:
                product_tmpl = False
        
            # Process variants for the template
            processed_count = 0
            for row in product_values_list:
                # Create a dictionary from the list for _create_or_update_variant
                variant_values = {}
                for col, index in column_map.items():
                    if index < len(row):
                        variant_values[col] = row[index]
                if product_tmpl:
                    variant = self._create_or_update_variant(product_tmpl, variant_values)
                    if variant:
                        processed_count += 1
                else:
                    # Create template on the fly if no template is defined
                    template_values = {}
                    for col, index in column_map.items():
                        if index < len(row):
                            template_values[col] = row[index]
                    template = self._create_product_template(template_values)
                    if template:
                        variant = self._create_or_update_variant(template, template_values)
                        if variant:
                            processed_count += 1
            _logger.info(f"=== Completed template processing. Processed {processed_count} variants ===")

    def _find_existing_template(self, template_values):
        """Find existing template by various identifiers."""
        template_ref = template_values.get('Template Internal Reference') or template_values.get('Internal Reference')
        if not template_ref:
            return None
            
        ProductTemplate = self.env['product.template']
        
        # If we have a reference, ONLY search by that reference
        if template_ref:
            _logger.info(f"Searching template by reference: {template_ref}")
            template = ProductTemplate.search([('default_code', '=', template_ref)], limit=1)
            if template:
                _logger.info(f"Found template by reference. ID: {template.id}, Name: {template.name}")
                return template
            else:
                _logger.info(f"No template found with reference: {template_ref}")
                return None
        
        # Only fall back to barcode/name matching if no reference was provided
        # Try finding by barcode first
        barcode = template_values.get('Barcode')
        if barcode:
            _logger.info(f"No reference provided. Searching template by barcode: {barcode}")
            template = ProductTemplate.search([('barcode', '=', barcode)], limit=1)
            if template:
                _logger.info(f"Found template by barcode. ID: {template.id}, Name: {template.name}")
                return template
                
        # Try finding by name as last resort
        name = template_values.get('Name')
        if name:
            _logger.info(f"No reference or barcode match. Searching template by name: {name}")
            template = ProductTemplate.search([('name', '=', name)], limit=1)
            if template:
                _logger.info(f"Found template by name. ID: {template.id}, Name: {template.name}")
                return template
                
        return None

    def _find_variant_by_combination(self, product_tmpl, values):
        """Find variant by its attribute combination"""
        _logger.info(f"Finding variant by combination for template {product_tmpl.name}")
        
        # Get all possible variants
        variants = self.env['product.product'].search([
            ('product_tmpl_id', '=', product_tmpl.id)
        ])
        
        if not variants:
            _logger.info("No variants found for template")
            return False
            
        # Extract attribute values from the import data
        import_attr_values = []
        for attr in product_tmpl.attribute_line_ids:
            attr_name = attr.attribute_id.name
            attr_value = values.get(attr_name)
            if not attr_value:
                _logger.warning(f"Missing value for attribute {attr_name}")
                continue
            import_attr_values.append(attr_value.strip())
            
        if not import_attr_values:
            _logger.warning("No attribute values found in import data")
            return False
            
        # Sort both lists for consistent comparison
        import_attr_values = sorted(import_attr_values)
        
        # Find matching variant
        for variant in variants:
            variant_attr_values = []
            for value in variant.product_template_attribute_value_ids:
                variant_attr_values.append(value.product_attribute_value_id.name)
            variant_attr_values = sorted(variant_attr_values)
            
            if import_attr_values == variant_attr_values:
                _logger.info(f"Found matching variant with combination: {', '.join(variant_attr_values)}")
                return variant
                
        _logger.info(f"No matching variant found for combination: {', '.join(import_attr_values)}")
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
                    variant = self.env['product.product'].create({
                        'product_tmpl_id': product_tmpl.id,
                        'combination_indices': ','.join(map(str, sorted(value_combination)))
                    })
                else:
                    # For default variant (no attribute values), reuse the default variant if available
                    if product_tmpl.product_variant_id:
                        variant = product_tmpl.product_variant_id
                        _logger.info(f'Reusing existing default variant for {product_tmpl.name}')
                    else:
                        # Fallback: search for existing default variant using proper NULL handling
                        variant = self.env['product.product'].search([
                            ('product_tmpl_id', '=', product_tmpl.id),
                            '|', ('combination_indices', '=', ''),
                                 ('combination_indices', '=', False)
                        ], limit=1)

                        if not variant:
                            # Use Odoo's native method with proper error handling
                            try:
                                with self.env.cr.savepoint():
                                    variant = product_tmpl._create_product_variant(False)
                                    _logger.info(f'Successfully created default variant for {product_tmpl.name}')
                            except Exception as create_error:
                                _logger.error(f'Failed to create default variant: {create_error}')
                                return False

            except Exception as e:
                _logger.error(f"Error creating variant: {e}")
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
        
        # Handle attribute values
        if values.get('Variant Attributes') and values.get('Attribute Values'):
            attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
            attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
            
            if len(attribute_names) != len(attribute_values):
                _logger.warning(f"Mismatch in attribute counts for {variant.product_tmpl_id.name}")
                return False

            # Get all product template attribute lines
            template_attribute_lines = variant.product_tmpl_id.attribute_line_ids

            # Collect attribute value IDs in the order they appear in the template
            attribute_value_ids = []
            for attr_name, attr_value in zip(attribute_names, attribute_values):
                # Find the attribute line for this attribute
                attr_line = template_attribute_lines.filtered(
                    lambda l: l.attribute_id.name == attr_name
                )
                if not attr_line:
                    _logger.warning(f"Attribute {attr_name} not found in template {variant.product_tmpl_id.name}")
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
                    ('product_tmpl_id', '=', variant.product_tmpl_id.id),
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
        
        # Handle attribute values
        if values.get('Variant Attributes') and values.get('Attribute Values'):
            attribute_names = [name.strip() for name in values['Variant Attributes'].split(',')]
            attribute_values = [value.strip() for value in values['Attribute Values'].split(';')]
            
            if len(attribute_names) != len(attribute_values):
                _logger.warning(f"Mismatch in attribute counts for {variant.product_tmpl_id.name}")
                return False

            # Get all product template attribute lines
            template_attribute_lines = variant.product_tmpl_id.attribute_line_ids

            # Collect attribute value IDs in the order they appear in the template
            attribute_value_ids = []
            for attr_name, attr_value in zip(attribute_names, attribute_values):
                # Find the attribute line for this attribute
                attr_line = template_attribute_lines.filtered(
                    lambda l: l.attribute_id.name == attr_name
                )
                if not attr_line:
                    _logger.warning(f"Attribute {attr_name} not found in template {variant.product_tmpl_id.name}")
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
                    ('product_tmpl_id', '=', variant.product_tmpl_id.id),
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
        """Create external ID for a record, handling duplicates gracefully"""
        if not record or not external_id:
            return False
            
        IrModelData = self.env['ir.model.data']
        
        # Clean the external_id to be XML-ID compatible
        clean_external_id = re.sub(r'[^a-zA-Z0-9_]', '_', external_id.lower())
        
        # Check if external ID already exists
        existing = IrModelData.search([
            ('model', '=', record._name),
            ('res_id', '=', record.id)
        ], limit=1)
        
        if existing:
            # Update existing record if needed
            if existing.name != clean_external_id:
                try:
                    existing.write({'name': clean_external_id})
                except Exception as e:
                    _logger.warning(f"Could not update external ID: {e}")
            return existing
            
        # Try to create new external ID
        try:
            return IrModelData.create({
                'model': record._name,
                'res_id': record.id,
                'module': '__import__',
                'name': clean_external_id,
            })
        except Exception as e:
            _logger.warning(f"Could not create external ID: {e}")
            # If creation fails, try with a unique suffix
            try:
                unique_name = f"{clean_external_id}_{record.id}"
                return IrModelData.create({
                    'model': record._name,
                    'res_id': record.id,
                    'module': '__import__',
                    'name': unique_name,
                })
            except Exception as e:
                _logger.error(f"Failed to create external ID with unique suffix: {e}")
                return False

    def _get_category_id(self, category_path):
        """Get or create product category from path."""
        if not category_path:
            return self.env.ref('product.product_category_all').id
            
        categories = category_path.split('/')
        parent_id = None
        current_id = None
        
        for cat_name in categories:
            cat_name = cat_name.strip()
            if not cat_name:
                continue
                
            domain = [('name', '=', cat_name)]
            if parent_id:
                domain.append(('parent_id', '=', parent_id))
                
            category = self.env['product.category'].search(domain, limit=1)
            if not category:
                category = self.env['product.category'].create({
                    'name': cat_name,
                    'parent_id': parent_id
                })
            parent_id = category.id
            current_id = category.id
            
        return current_id or self.env.ref('product.product_category_all').id

    def _prepare_template_values(self, template_values):
        """Prepare values for creating a new product template"""
        vals = {
            'name': template_values.get('Name', ''),
            'default_code': template_values.get('Template Internal Reference') or template_values.get('Internal Reference'),
            'barcode': template_values.get('Barcode'),
            'type': 'product',
            'categ_id': self._get_category_id(template_values.get('Category')),
            'sale_ok': template_values.get('Canbe Sold', 'TRUE').upper() == 'TRUE',
            'purchase_ok': template_values.get('Canbe Purchased', 'TRUE').upper() == 'TRUE',
            'available_in_pos': template_values.get('Available in POS', 'TRUE').upper() == 'TRUE',
        }
        
        # Prepare and set attribute lines
        attr_lines = self._prepare_attribute_lines(template_values, template_values)
        if attr_lines:
            vals['attribute_line_ids'] = attr_lines
            
        return vals

    def _prepare_attribute_lines(self, template_values, template):
        """Prepare attribute lines for product template from import values.
        Args:
            template_values (dict): The values from the import file
            template (product.template): The template record to add attributes to
        Returns:
            list: List of attribute line commands to write to the template
        """
        if not template_values.get('Variant Attributes') or not template_values.get('Attribute Values'):
            return []
            
        attributes = template_values['Variant Attributes'].split(',')
        values_list = template_values['Attribute Values'].split(';')
        
        if len(attributes) != len(values_list):
            _logger.warning(f"Mismatch in attribute count ({len(attributes)}) and values count ({len(values_list)})")
            return []
            
        ProductAttribute = self.env['product.attribute']
        ProductAttributeValue = self.env['product.attribute.value']
        
        attr_lines = []
        for attribute_name, value_name in zip(attributes, values_list):
            attribute = ProductAttribute.search([('name', '=', attribute_name.strip())], limit=1)
            if not attribute:
                _logger.warning(f"Attribute {attribute_name} not found")
                continue
                
            value = ProductAttributeValue.search([
                ('name', '=', value_name.strip()),
                ('attribute_id', '=', attribute.id)
            ], limit=1)
            
            if not value:
                _logger.info(f"Creating attribute value {value_name} for attribute {attribute_name}")
                value = ProductAttributeValue.create({
                    'name': value_name.strip(),
                    'attribute_id': attribute.id
                })
                
            existing_line = template.attribute_line_ids.filtered(
                lambda l: l.attribute_id == attribute
            )
            
            if existing_line:
                if value not in existing_line.value_ids:
                    existing_line.value_ids = [(4, value.id)]
            else:
                attr_lines.append((0, 0, {
                    'attribute_id': attribute.id,
                    'value_ids': [(4, value.id)]
                }))
                
        return attr_lines

    def _create_product_template(self, template_values):
        """Create a new product template with the given values."""
        ProductTemplate = self.env['product.template']
        
        # Check one more time with a clean transaction
        template_ref = template_values.get('Template Internal Reference') or template_values.get('Internal Reference')
        self.env.cr.commit()  # Commit any pending transactions
        existing = ProductTemplate.search([('default_code', '=', template_ref)], limit=1)
        if existing:
            _logger.info(f"Found template in new transaction. ID: {existing.id}, Name: {existing.name}")
            return existing
        
        # Get category
        category_id = self._get_category_id(template_values.get('Category'))
        
        vals = {
            'name': template_values['Name'],
            'default_code': template_ref,
            'barcode': template_values.get('Barcode'),
            'type': 'product',
            'categ_id': category_id,
            'sale_ok': template_values.get('Canbe Sold', 'TRUE').upper() == 'TRUE',
            'purchase_ok': template_values.get('Canbe Purchased', 'TRUE').upper() == 'TRUE',
            'available_in_pos': template_values.get('Available in POS', 'TRUE').upper() == 'TRUE',
        }
        
        # Process image if provided
        image_url = template_values.get('Image')
        if image_url:
            _logger.info(f"Processing image from URL: {image_url}")
            try:
                response = requests.get(image_url, timeout=10)
                if response.status_code == 200:
                    image_data = base64.b64encode(response.content)
                    vals['image_1920'] = image_data.decode('utf-8')
                    _logger.info(f"Successfully processed image from {image_url}")
                else:
                    _logger.warning(f"Failed to fetch image from {image_url}: Status code {response.status_code}")
            except Exception as e:
                _logger.warning(f"Error processing image {image_url}: {str(e)}")
        else:
            _logger.info("No image URL found in template values")
            _logger.debug(f"Available fields in template_values: {list(template_values.keys())}")
        
        template = ProductTemplate.create(vals)
        self.env.cr.commit()  # Commit immediately after creation
        
        # Double check the template was created
        template = ProductTemplate.browse(template.id)
        if not template.exists():
            _logger.error(f"Failed to create template with reference: {template_ref}")
            return None
            
        _logger.info(f"Successfully created template. ID: {template.id}, Name: {template.name}")
        
        # Prepare and set attribute lines
        attr_lines = self._prepare_attribute_lines(template_values, template)
        if attr_lines:
            template.write({'attribute_line_ids': attr_lines})
            self.env.cr.commit()  # Commit after adding attribute lines
            
        return template

    def _create_or_update_variant(self, template, values):
        """Create or update a product variant."""
        ProductProduct = self.env['product.product']
        
        # Extract attribute values from the import data
        if values.get('Variant Attributes') and values.get('Attribute Values'):
            attributes = values['Variant Attributes'].split(',')
            value_names = values['Attribute Values'].split(';')
            
            # Create a dict of attribute_id: value_id pairs
            value_pairs = []
            for attr_name, value_name in zip(attributes, value_names):
                attribute = self.env['product.attribute'].search([('name', '=', attr_name.strip())], limit=1)
                if not attribute:
                    continue
                    
                value = self.env['product.attribute.value'].search([
                    ('name', '=', value_name.strip()),
                    ('attribute_id', '=', attribute.id)
                ], limit=1)
                
                if value:
                    value_pairs.append((attribute.id, value.id))
                    
            # Find variant by attribute value combination
            domain = [('product_tmpl_id', '=', template.id)]
            for attr_id, value_id in value_pairs:
                domain.append(('product_template_attribute_value_ids.product_attribute_value_id', '=', value_id))
                
            variant = ProductProduct.search(domain, limit=1)
            
            if not variant:
                _logger.info(f"Creating new variant for template {template.name}")
                variant = ProductProduct.create({
                    'product_tmpl_id': template.id,
                    'default_code': values.get('Internal Reference'),
                    'barcode': values.get('Barcode'),
                })
            
            # Update variant values
            variant.write({
                'default_code': values.get('Internal Reference'),
                'barcode': values.get('Barcode'),
            })
            
            return variant
        return False

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

    def _prepare_attribute_lines(self, values, template):
        """Prepare attribute lines for product template from import values."""
        if not values.get('Variant Attributes') or not values.get('Attribute Values'):
            return []
            
        attributes = values['Variant Attributes'].split(',')
        values_list = values['Attribute Values'].split(';')
        
        if len(attributes) != len(values_list):
            _logger.warning(f"Mismatch in attribute count ({len(attributes)}) and values count ({len(values_list)})")
            return []
            
        ProductAttribute = self.env['product.attribute']
        ProductAttributeValue = self.env['product.attribute.value']
        
        attr_lines = []
        for attribute_name, value_name in zip(attributes, values_list):
            attribute = ProductAttribute.search([('name', '=', attribute_name.strip())], limit=1)
            if not attribute:
                _logger.warning(f"Attribute {attribute_name} not found")
                continue
                
            value = ProductAttributeValue.search([
                ('name', '=', value_name.strip()),
                ('attribute_id', '=', attribute.id)
            ], limit=1)
            
            if not value:
                _logger.info(f"Creating attribute value {value_name} for attribute {attribute_name}")
                value = ProductAttributeValue.create({
                    'name': value_name.strip(),
                    'attribute_id': attribute.id
                })
                
            existing_line = template.attribute_line_ids.filtered(
                lambda l: l.attribute_id == attribute
            )
            
            if existing_line:
                if value not in existing_line.value_ids:
                    existing_line.value_ids = [(4, value.id)]
            else:
                attr_lines.append((0, 0, {
                    'attribute_id': attribute.id,
                    'value_ids': [(4, value.id)]
                }))
                
        return attr_lines

    def _update_product_template(self, template, template_values):
        """Update an existing product template with new values."""
        vals = {
            'name': template_values['Name'],
            'default_code': template_values.get('Template Internal Reference') or template_values.get('Internal Reference'),
            'barcode': template_values.get('Barcode'),
            'sale_ok': template_values.get('Canbe Sold', 'TRUE').upper() == 'TRUE',
            'purchase_ok': template_values.get('Canbe Purchased', 'TRUE').upper() == 'TRUE',
            'available_in_pos': template_values.get('Available in POS', 'TRUE').upper() == 'TRUE',
        }
        
        # Process image if provided
        image_url = template_values.get('Image')
        if image_url:
            _logger.info(f"Processing image from URL: {image_url}")
            try:
                response = requests.get(image_url, timeout=10)
                if response.status_code == 200:
                    image_data = base64.b64encode(response.content)
                    vals['image_1920'] = image_data.decode('utf-8')
                    _logger.info(f"Successfully processed image from {image_url}")
                else:
                    _logger.warning(f"Failed to fetch image from {image_url}: Status code {response.status_code}")
            except Exception as e:
                _logger.warning(f"Error processing image {image_url}: {str(e)}")
        else:
            _logger.info("No image URL found in template values")
            _logger.debug(f"Available fields in template_values: {list(template_values.keys())}")
        
        template.write(vals)
        
        # Update attribute lines
        attr_lines = self._prepare_attribute_lines(template_values, template)
        if attr_lines:
            template.write({'attribute_line_ids': attr_lines})
            
        return template

    def _find_existing_template(self, template_values):
        """Find existing template by various identifiers."""
        template_ref = template_values.get('Template Internal Reference') or template_values.get('Internal Reference')
        if not template_ref:
            return None
            
        ProductTemplate = self.env['product.template']
        
        # If we have a reference, ONLY search by that reference
        if template_ref:
            _logger.info(f"Searching template by reference: {template_ref}")
            template = ProductTemplate.search([('default_code', '=', template_ref)], limit=1)
            if template:
                _logger.info(f"Found template by reference. ID: {template.id}, Name: {template.name}")
                return template
            else:
                _logger.info(f"No template found with reference: {template_ref}")
                return None
        
        # Only fall back to barcode/name matching if no reference was provided
        # Try finding by barcode first
        barcode = template_values.get('Barcode')
        if barcode:
            _logger.info(f"No reference provided. Searching template by barcode: {barcode}")
            template = ProductTemplate.search([('barcode', '=', barcode)], limit=1)
            if template:
                _logger.info(f"Found template by barcode. ID: {template.id}, Name: {template.name}")
                return template
                
        # Try finding by name as last resort
        name = template_values.get('Name')
        if name:
            _logger.info(f"No reference or barcode match. Searching template by name: {name}")
            template = ProductTemplate.search([('name', '=', name)], limit=1)
            if template:
                _logger.info(f"Found template by name. ID: {template.id}, Name: {template.name}")
                return template
                
        return None