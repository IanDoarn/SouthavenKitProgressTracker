import xlsxwriter
import copy
import json
import re

XLSX_FILE = 'NEW_KIT_STOCK_TRACKER.xlsx'
FILE = '3DAY_NEW_KIT_STOCK.TXT'
J_FILE = '3DAY_NEW_KIT_STOCK.json'

with open(FILE, 'r')as f:
    n_data = f.readlines()

with open(J_FILE, 'r')as jf:
    j_data = json.load(jf)

n_data = [x.replace('\n', '') for x in n_data]
n_kits = {}
j_kits = copy.deepcopy(j_data['data'])

for l in n_data:
    m = re.search(r"^([\d\w-]*)(\s{3})(serials) ([\d,]*)$", l)
    n_kits[m.group(1)] = m.group(4).split(',')

for k, v in n_kits.items():
    if k in j_kits.keys():
        j_kits[k].extend(v)
    else:
        j_kits[k] = v

for k, v in j_kits.items():
    j_kits[k] = sorted(list(set([int(x) for x in v])))

data = copy.deepcopy(j_kits)
kits = sorted(list(data.keys()))
serials = sorted([int(x) for x in set(sum(data.values(), []))])

with open(J_FILE, 'w')as jf:
    json.dump(
        {'data': data, 'kits': kits, 'serials': serials},
        jf, indent=4, ensure_ascii=True
    )

workbook = xlsxwriter.Workbook(XLSX_FILE)
worksheet = workbook.add_worksheet(name='NEW_KITS')

xlsx_data = [
    [k, ', '.join([str(x) for x in v])] for k, v in data.items()
]


worksheet.add_table(
    'A1:B' + str(len(data) + 1),
    {
        'data': xlsx_data,
        'columns': [
            {'header': 'KIT_NUMBER'},
            {'header': 'SERIAL_NUMBERS'}
        ]
    }
)

workbook.close()

print('{} KIT NUMBERS TRACKED || {} SERIALS'.format(
    str(len(kits)),
    str(len(sum(data.values(), [])))
))