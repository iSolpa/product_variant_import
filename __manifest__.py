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
{
    'name': 'Import Product variant',
    'version': '17.0.1.2.1',
    'category': 'Sales',
    'summary': """Enhanced module for importing products and product variants with advanced features.""",
    'description': """This module is used to import products and their attributes using xlsx and csv files.
    Originally developed by Cybrosys Technologies and enhanced by Independent Solutions (iSolpa.com).
    
    Features include:
    * Import products with attributes using xlsx or csv files
    * Support for basic fields (char, many2one, many2many)
    * Product updates using internal reference or barcode
    * Enhanced data validation and error handling
    * Improved user interface and usability
    """,
    'author': 'Cybrosys Techno Solutions, Independent Solutions',
    'company': 'Independent Solutions',
    'maintainer': 'Independent Solutions',
    'website': 'https://isolpa.com',
    'depends': ['sale_management', 'stock', 'point_of_sale'],
    'data': [
        'security/ir.model.access.csv',
        'security/product_variant_import_groups.xml',
        'wizards/import_product_variant_views.xml',
    ],
    'images': ['static/description/banner.png'],
    'license': 'LGPL-3',
    'installable': True,
    'auto_install': False,
    'application': False,
}
