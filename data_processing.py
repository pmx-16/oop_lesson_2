import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

titanics = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanics.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

table1 = Table('titanics', titanics)
my_DB = DB()
my_DB.insert(table1)
my_table1 = my_DB.search('titanics')

first_fare = []
my_table1_filtered = my_table1.filter(lambda x: float(x['class']) == 1)
for item1 in my_table1_filtered.table:
    first_fare.append(float(item1['fare']))

third_fare = []
my_table2_filtered = my_table1.filter(lambda x: float(x['class']) == 3)
for item2 in my_table2_filtered.table:
    third_fare.append(float(item2['fare']))

first_avg = sum(first_fare)/len(first_fare)
third_avg = sum(third_fare)/len(third_fare)

print(f"average fare paid by first class customers: {first_avg}")
print(f"average fare paid by third class customers: {third_avg}")

male = []
my_table3_filtered = my_table1.filter(lambda x: x['gender'] == 'M')
for male1 in my_table3_filtered.table:
    male.append(str(male1['first']))

rate_M = []
my_table4_filtered = my_table1.filter(lambda x: x['gender'] == 'M').filter(lambda x: x['survived'] == 'yes')
for rate1 in my_table4_filtered.table:
    rate_M.append(str(rate1['first']))

female = []
my_table5_filtered = my_table1.filter(lambda x: x['gender'] == 'F')
for female1 in my_table5_filtered.table:
    female.append(str(female1['first']))

rate_F = []
my_table6_filtered = my_table1.filter(lambda x: x['gender'] == 'F').filter(lambda x: x['survived'] == 'yes')
for rate2 in my_table6_filtered.table:
    rate_F.append(str(rate2['first']))

surv_rate_M = (len(rate_M)/len(male)) * 100
surv_rate_F = (len(rate_F))/len(female) *100
print(f"survival rate of male passengers: {surv_rate_M}")
print(f"survival rate of female passengers: {surv_rate_F}")




