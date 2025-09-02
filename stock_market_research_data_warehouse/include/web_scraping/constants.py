CSV_HOLDINGS_SELECTORS = [
    '#holdings .fund-component-data-export a.icon-xls-export[href*="csv"]',
    '#holdings a.icon-xls-export[href*="holdings"]',
    "#holdings a.icon-xls-export",
    'a.icon-xls-export[href*="holdings&fileType=csv"]',
    'a.icon-xls-export[href*="fileType=csv"]',
    "a.icon-xls-export",
    'a[data-download-type="holdings"][href*="csv"]',
]

ANY_DOWNLOAD_SELECTORS = [
    "a.icon-xls-export",
]
