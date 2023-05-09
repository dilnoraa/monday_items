from import_items import MondayDeals
import os
from django.conf import settings


if __name__ == '__main__':
    monday = MondayDeals()
    file_path = os.path.join(settings.ROOT_PATH, 'monday_deals', "import_leads_to_crm.xlsx")
    monday.import_deals_to_board(file_path)


