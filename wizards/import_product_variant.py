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
        conflicting_product = self.env['product.template'].search([
            ('id', '!=', product.id if product else self.id),
            '|',
            ('barcode', '=', barcode),
            ('product_variant_ids.barcode', '=', barcode)
        ], limit=1)
        return not conflicting_product, conflicting_product

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
        """Process a single product template and its variants."""
        _logger.info(f"Processing product template: {group_key}")
        # Use the first values as the template data
        template_values = product_values_list[0]
        # Initialize vals dictionary for both create and update methods
        vals = {
            'name': template_values.get('Name', ''),
            'default_code': template_values.get('Internal Reference', ''),
            'barcode': template_values.get('Barcode', ''),
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

        # Process image if provided
        if template_values.get('Image'):
            image_path = template_values.get('Image')
            image_data = po.process_image(image_path)
            if image_data:
                vals['image_1920'] = image_data

        # Process POS Category if provided
        if template_values.get('POS Category'):
            pos_categ = self.env['pos.category'].search([('name', '=', template_values['POS Category'])], limit=1)
            if not pos_categ:
                pos_categ = self.env['pos.category'].create({
                    'name': template_values['POS Category']
                })
            vals['pos_categ_ids'] = [(6, 0, [pos_categ.id])]

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

        # Find or create product template based on method
        if self.method in ['update', 'update_product']:
            product = False
            search_domain = ['|', '|']  # Start with OR operators for three conditions
            
            if template_values.get('Barcode'):
                search_domain.append(('barcode', '=', template_values['Barcode']))
            else:
                search_domain.append(('barcode', '=', False))
                
            if template_values.get('Internal Reference'):
                search_domain.append(('default_code', '=', template_values['Internal Reference']))
            else:
                search_domain.append(('default_code', '=', False))
                
            if template_values.get('Unique Identifier'):
                search_domain.append(('default_code', '=', template_values['Unique Identifier']))
            else:
                search_domain.append(('default_code', '=', False))
            
            _logger.info(f"Searching for product with domain: {search_domain}")
            product = self.env['product.template'].search(search_domain, limit=1)
            
            if not product:
                _logger.warning(_(
                    "Skipping update - Product not found with:\n"
                    "Barcode: %s\n"
                    "Internal Reference: %s\n"
                    "Unique Identifier: %s"
                ) % (
                    template_values.get('Barcode', 'N/A'),
                    template_values.get('Internal Reference', 'N/A'),
                    template_values.get('Unique Identifier', 'N/A')
                ))
                return
            
            _logger.info(f"Found existing product template: {group_key} (ID: {product.id})")
            
            # Ensure barcode conflict check happens before any write/create
            barcode = template_values.get('Barcode', '')
            if barcode:
                can_use_barcode, conflicting_product = self._check_barcode_conflicts(barcode, product)
                if not can_use_barcode:
                    _logger.warning(
                        f"Barcode {barcode} already assigned to {conflicting_product.display_name}. "
                        f"Skipping barcode update for template."
                    )
                    vals.pop('barcode')
            
            # Update template with remaining values
            product.write(vals)
        else:
            # Check if product already exists
            existing_product = False
            if template_values.get('Barcode'):
                existing_product = self.env['product.template'].search([
                    '|',
                    ('barcode', '=', template_values['Barcode']),
                    ('product_variant_ids.barcode', '=', template_values['Barcode'])
                ], limit=1)
            
            if existing_product:
                _logger.warning(_(
                    "Skipping creation - Barcode '%s' already assigned to product: [%s] %s"
                ) % (
                    template_values.get('Barcode'),
                    existing_product.default_code or existing_product.barcode or 'N/A',
                    existing_product.display_name
                ))
                return
            
            # If no existing product found, create new one
            product = self.env['product.template'].create(vals)
            _logger.info(f"Created new product template: {group_key} (ID: {product.id})")

        # Prepare attribute lines and variant data
        attribute_value_mapping = {}
        variant_specific_values_list = []
        for values in product_values_list:
            # Process variants
            if values.get('Variant Attributes') and values.get('Attribute Values'):
                attributes = values['Variant Attributes'].split(',')
                values_list = values['Attribute Values'].split(';')
                
                if len(attributes) != len(values_list):
                    raise UserError(_(
                        "Number of attributes ({}) does not match number of values ({}) for variant of product '{}'"
                    ).format(len(attributes), len(values_list), group_key))
                
                # Store the combination of values for this variant
                value_combination = tuple(v.strip() for v in values_list)
                specific_values = {
                    'default_code': values.get('Internal Reference', '').strip(),
                }
                
                # Handle quantity from either column name
                qty_value = values.get('Qty On hand', values.get('Quantity', ''))
                # Only process quantity if it's not blank
                if qty_value.strip() if isinstance(qty_value, str) else qty_value:
                    specific_values['qty_available'] = float(qty_value or '0.0')
                
                # Only add barcode if it's not in the skip list
                barcode = values.get('Barcode', '').strip()
                if barcode:
                    specific_values['barcode'] = barcode
                
                variant_specific_values_list.append({
                    'value_combination': value_combination,
                    'specific_values': specific_values,
                })
                
                # Collect attribute values
                for attr_name, attr_value in zip(attributes, values_list):
                    attr_name = attr_name.strip()
                    attr_value = attr_value.strip()
                    if attr_name not in attribute_value_mapping:
                        attribute_value_mapping[attr_name] = set()
                    attribute_value_mapping[attr_name].add(attr_value)
            else:
                _logger.warning(f"No variant attributes or attribute values provided for product '{group_key}' in some rows.")

        if attribute_value_mapping:
            # Remove existing attribute lines only after collecting all values
            if self.method in ['update', 'update_product']:
                existing_attr_lines = self.env['product.template.attribute.line'].search([
                    ('product_tmpl_id', '=', product.id)
                ])
                if existing_attr_lines:
                    _logger.info(f"Removing existing attribute lines for product {group_key}")
                    existing_attr_lines.unlink()
            
            # Create attribute lines for each attribute with all its values
            attribute_lines = []
            for attr_name, attr_values in attribute_value_mapping.items():
                _logger.info(f"Processing attribute '{attr_name}' with values: {attr_values}")
                
                # Find or create attribute
                attribute = self.env['product.attribute'].search([('name', '=', attr_name)], limit=1)
                if not attribute:
                    attribute = self.env['product.attribute'].create({
                        'name': attr_name,
                        'create_variant': 'always'
                    })
                _logger.info(f"Using attribute '{attr_name}' (ID: {attribute.id})")
                
                # Find or create all values for this attribute
                value_ids = []
                for attr_value in attr_values:
                    attr_value_obj = self.env['product.attribute.value'].search([
                        ('name', '=', attr_value),
                        ('attribute_id', '=', attribute.id)
                    ], limit=1)
                    if not attr_value_obj:
                        attr_value_obj = self.env['product.attribute.value'].create({
                            'name': attr_value,
                            'attribute_id': attribute.id
                        })
                    value_ids.append(attr_value_obj.id)
                    _logger.info(f"Added value '{attr_value}' (ID: {attr_value_obj.id})")
                
                # Create attribute line with all values
                if value_ids:
                    attr_line = self.env['product.template.attribute.line'].create({
                        'product_tmpl_id': product.id,
                        'attribute_id': attribute.id,
                        'value_ids': [(6, 0, value_ids)]
                    })
                    attribute_lines.append(attr_line)
                    _logger.info(f"Created attribute line for '{attr_name}' with {len(value_ids)} values")
            
            # Wait for variants to be created
            product.invalidate_recordset()
            product._create_variant_ids()
            product.flush_recordset()
            self.env.cr.commit()  # Ensure all changes are committed
            
            # Force a reload of the product to ensure we have all variants
            product = self.env['product.template'].browse(product.id)
            
            # Update variant-specific values
            if variant_specific_values_list:
                _logger.info(f"Updating variant-specific values for {len(variant_specific_values_list)} combinations")
                
                # Get all existing variants and their combinations
                variants = self.env['product.product'].search([('product_tmpl_id', '=', product.id)])
                _logger.info(f"Found {len(variants)} variants for product {product.name}")
                
                variant_map = {}
                for variant in variants:
                    # Create both full and partial combinations for flexible matching
                    value_combinations = []
                    sorted_values = variant.product_template_attribute_value_ids.sorted('attribute_id')
                    
                    # Add the full combination
                    full_combination = tuple(value.name for value in sorted_values)
                    value_combinations.append(full_combination)
                    
                    # Add partial combinations based on attribute names
                    attr_values = [(value.attribute_id.name, value.name) for value in sorted_values]
                    for i in range(len(attr_values)):
                        partial_values = []
                        for attr_name, value_name in attr_values:
                            if attr_name in ['Talla', 'Colores']:  # Always include size and color
                                partial_values.append(value_name)
                        if partial_values:
                            value_combinations.append(tuple(partial_values))
                    
                    # Map all combinations to this variant
                    for combination in value_combinations:
                        variant_map[combination] = variant
                        _logger.info(f"Mapped variant combination {combination} to variant {variant.display_name}")

                # Get the default location for inventory adjustments
                location = self.env['stock.location'].search([
                    ('usage', '=', 'internal'),
                    ('company_id', '=', self.env.company.id)
                ], limit=1)
                
                if not location:
                    _logger.error("No internal location found for the current company. Inventory adjustment cannot be created.")
                    return  # Exit the function if no location is found
                
                # Process variants
                for variant_data in variant_specific_values_list:
                    value_combination = variant_data['value_combination']
                    specific_values = variant_data['specific_values']
                    variant = variant_map.get(value_combination)
                    
                    if not variant:
                        # If variant doesn't exist, create it
                        try:
                            _logger.info(f"Creating missing variant with combination {value_combination}")
                            
                            # Create the variant through product template
                            template_attribute_values = []
                            for value_name in value_combination:
                                for attr_line in product.attribute_line_ids:
                                    attr_value = self.env['product.attribute.value'].search([
                                        ('name', '=', value_name),
                                        ('attribute_id', '=', attr_line.attribute_id.id)
                                    ], limit=1)
                                    if attr_value:
                                        # Find the product template attribute value
                                        ptav = self.env['product.template.attribute.value'].search([
                                            ('product_attribute_value_id', '=', attr_value.id),
                                            ('attribute_line_id', 'in', product.attribute_line_ids.ids)
                                        ], limit=1)
                                        if ptav:
                                            template_attribute_values.append(ptav.id)
                            
                            if len(template_attribute_values) == len(value_combination):
                                # Check if a variant with these template attribute values already exists
                                existing_variant = self.env['product.product'].search([
                                    ('product_tmpl_id', '=', product.id),
                                    ('product_template_attribute_value_ids', 'in', template_attribute_values)
                                ], limit=1)
                                
                                if existing_variant:
                                    _logger.info(f"Found existing variant with same combination. Updating instead of creating.")
                                    variant = existing_variant
                                else:
                                    # Create the variant with the template attribute values
                                    variant_vals = {
                                        'product_tmpl_id': product.id,
                                        'product_template_attribute_value_ids': [(6, 0, template_attribute_values)]
                                    }
                                    if specific_values.get('default_code'):
                                        variant_vals['default_code'] = specific_values['default_code']
                                    if specific_values.get('barcode'):
                                        variant_vals['barcode'] = specific_values['barcode']
                                    
                                    variant = self.env['product.product'].create(variant_vals)
                                    _logger.info(f"Successfully created variant {variant.display_name}")
                            else:
                                _logger.error(f"Could not find all template attribute values for combination {value_combination}")
                        except Exception as e:
                            _logger.error(f"Failed to create variant with combination {value_combination}: {str(e)}", exc_info=True)
                            continue
                    
                    if variant:
                        # Ensure barcode conflict check happens before any variant updates
                        if 'barcode' in specific_values:
                            can_use_barcode, conflicting_product = self._check_barcode_conflicts(specific_values['barcode'], variant)
                            if not can_use_barcode:
                                _logger.warning(
                                    f"Barcode {specific_values['barcode']} already assigned to {conflicting_product.display_name}")
                                continue  # Skip this variant if barcode conflict exists
                        
                        # Proceed with variant update only after conflict check
                        if specific_values:  # Only update if there are values to update
                            try:
                                variant.write(specific_values)
                                _logger.info(f"Updated variant {variant.display_name} with values {specific_values}")
                                
                                # Handle quantity updates through stock.quant
                                if 'qty_available' in specific_values:
                                    qty = float(specific_values.get('qty_available', 0.0))
                                    current_qty = variant.with_context(location=location.id).qty_available if location else 0.0
                                    
                                    if location:
                                        if float_compare(qty, current_qty, precision_digits=2) != 0:
                                            _logger.info(f"Creating stock quant for variant {variant.display_name}")
                                            try:
                                                # Create stock quant
                                                quant = self.env['stock.quant'].create({
                                                    'product_id': variant.id,
                                                    'location_id': location.id,
                                                    'inventory_quantity': qty,
                                                })
                                                _logger.info(f"Created stock quant for {variant.display_name} (ID: {quant.id})")
                                                
                                                # Apply inventory
                                                quant.action_apply_inventory()
                                                _logger.info(f"Applied inventory for {variant.display_name}")
                                            except Exception as e:
                                                _logger.error(f"Error during stock quant adjustment for variant {variant.display_name}: {str(e)}", exc_info=True)
                                else:
                                    _logger.info(f"Skipping quantity update for variant {variant.display_name} (no quantity specified in import)")
                            except Exception as e:
                                _logger.error(f"Failed to update variant {variant.display_name}: {str(e)}", exc_info=True)
        else:
            _logger.info(f"No variant attributes provided for product '{group_key}'")

    def _get_selection_key(self, field_name, value):
        """Get selection key from field selection."""
        field_selection = dict(self.env['product.template']._fields[field_name].selection)
        for key, val in field_selection.items():
            if val == value:
                return key
        return None
