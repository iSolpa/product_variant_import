import base64
import logging
import os
import requests
from odoo import _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

def process_category(env, category_name):
    """Process product category with proper space handling."""
    # Trim spaces from the entire category name
    category_name = category_name.strip()
    
    category = env['product.category'].search([
        '|',
        ('complete_name', '=', category_name),
        ('name', '=', category_name)
    ], limit=1)
    
    if category:
        return category.id
    else:
        # Create new category if it doesn't exist
        # Split by / and trim spaces for each category level
        name = category_name.split('/')[0].strip()
        category = env['product.category'].create({
            'name': name,
            'complete_name': category_name
        })
        return category.id

def process_uom(env, uom_name):
    """Process unit of measure."""
    uom = env['uom.uom'].search([('name', '=', uom_name)]).id
    if not uom:
        raise UserError(_("Invalid uom"))
    return uom

def process_tax(env, tax_name, tax_type='sale'):
    """Process tax values with better error handling."""
    if not tax_name or tax_name == '':
        return False

    tax_name = str(tax_name).strip()
    tax = env['account.tax'].search([('name', '=', tax_name)], limit=1)
    if tax:
        return tax.id

    try:
        # Try to parse "name percentage" format
        parts = tax_name.split(' ')
        name = parts[0]
        amount = float(parts[1]) if len(parts) > 1 else 0.0
        
        tax = env['account.tax'].create({
            'name': name,
            'amount': amount,
            'type_tax_use': tax_type,
            'price_include': False,
            'company_id': env.company.id,
            'amount_type': 'percent',
        })
        return tax.id
    except (IndexError, ValueError):
        # If parsing fails, create with just the name
        tax = env['account.tax'].create({
            'name': tax_name,
            'amount': 0.0,
            'type_tax_use': tax_type,
            'price_include': False,
            'company_id': env.company.id,
            'amount_type': 'percent',
        })
        return tax.id

def process_image(image_path):
    """Process product image."""
    link = False
    if "http://" in image_path or "https://" in image_path:
        link = base64.b64encode(
            requests.get(image_path.strip()).content).replace(b"\n", b"")
    elif "/home" in image_path:
        if os.path.exists(image_path):
            with open(image_path, 'rb') as image_file:
                link = base64.b64encode(image_file.read())
    return link

def process_attributes(env, attribute_names, attribute_values):
    """Process product attributes and values."""
    values = []
    for attr_name in attribute_names.split(','):
        attr_name = attr_name.strip()
        attribute = env['product.attribute'].search([('name', '=', attr_name)]).id
        if not attribute:
            raise UserError(_("Attribute '%s' not found. Please verify it exists.") % attr_name)
        values.append({'attribute': attribute})
        
        # Process attribute values
        for value_name in attribute_values.split(','):
            value_name = value_name.strip()
            attr_value = env['product.attribute.value'].search([
                ('attribute_id', '=', attribute),
                ('name', '=', value_name)
            ]).ids
            if attr_value:
                values.extend(attr_value)
            else:
                # Create attribute value if it doesn't exist
                new_value = env['product.attribute.value'].create({
                    'name': value_name,
                    'attribute_id': attribute
                })
                values.extend([new_value.id])
    
    return values

def create_attribute_lines(env, product_id, values):
    """Create product attribute lines."""
    variant = {}
    mylist = []
    for val in values:
        if isinstance(val, dict):
            variant = val
            variant['attribut_value'] = []
        else:
            if 'attribut_value' not in variant:
                variant['attribut_value'] = []
            variant['attribut_value'].extend([val])
        if variant not in mylist:
            mylist.append(variant)
    
    for lst in mylist:
        val = {
            'product_tmpl_id': product_id,
            'attribute_id': lst['attribute'],
            'value_ids': [(6, 0, lst['attribut_value'])],
        }
        env['product.template.attribute.line'].create(val)

def check_barcode_conflicts(env, barcode, product_id=None):
    """Check for barcode conflicts."""
    if not barcode:
        return True, None
        
    domain = [('barcode', '=', barcode)]
    if product_id:
        domain.append(('id', '!=', product_id))
    
    conflicting_product = env['product.template'].search(domain + [
        '|',
        ('barcode', '=', barcode),
        ('product_variant_ids.barcode', '=', barcode)
    ], limit=1)
    
    return not bool(conflicting_product), conflicting_product
