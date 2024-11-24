import base64
import binascii
import csv
import io
import tempfile
import xlrd
from odoo import _
from odoo.exceptions import UserError

def process_excel_file(file_content):
    """Process Excel file and return rows."""
    try:
        file_pointer = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        file_pointer.write(binascii.a2b_base64(file_content))
        file_pointer.seek(0)
        workbook = xlrd.open_workbook(file_pointer.name)
        sheet = workbook.sheet_by_index(0)
        return [sheet.row_values(rec) for rec in range(1, sheet.nrows)]
    except Exception:
        raise UserError(_("File not Valid"))

def process_csv_file(file_content):
    """Process CSV file and return rows and column mapping."""
    try:
        files = base64.b64decode(file_content)
        file_reader = []
        
        # Handle CSV file
        data = io.StringIO(files.decode("utf-8"))
        data.seek(0)
        csv_reader = csv.reader(data, delimiter=',')
        file_reader.extend(csv_reader)
        
        if not file_reader:
            raise UserError(_("No data found in the file"))
        
        header = file_reader[0] if file_reader else []
        if not header:
            raise UserError(_("Empty header row in file"))
        
        # Create a mapping of column names to indices
        column_map = {str(col).strip(): idx for idx, col in enumerate(header) if col}
        
        return file_reader[1:], column_map
    except Exception as e:
        raise UserError(_("Error processing CSV file: %s") % str(e))

def validate_row_data(row, header_length, row_num, required_columns):
    """Validate row data, focusing on required columns."""
    if not row or len(row) < len(required_columns):
        return False, f"Skipping malformed row {row_num}: row length {len(row) if row else 0} < required columns length {len(required_columns)}"
    return True, None

def process_cell_value(value):
    """Process cell value and convert to appropriate format."""
    if isinstance(value, (float, int)):
        return str(value)
    return value.strip() if value else ''
