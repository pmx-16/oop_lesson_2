import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

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

table1 = Table('players', players)
table2 = Table('teams', teams)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_table1 = my_DB.search('players')
my_table2 = my_DB.search('teams')

print("Test filter: only filtering out players with less than 200 minutes and more than 100 passes") 
my_table1_filtered = my_table1.filter(lambda x: float(x['minutes']) < 200).filter(lambda x: float(x['passes']) > 100).filter(lambda player: "ia" in player["team"])
#print(my_table1_filtered)
#print()
print("Test select: only displaying surname, team, position")
my_table1_selected = my_table1_filtered.select(['surname', 'team', 'position'])
print(my_table1_selected)

game1 = []
my_table2_filtered = my_table2.filter(lambda x: float(x["ranking"]) < 10)
for item in my_table2_filtered.table:
    game1.append(float(item['games']))

game2 = []
my_table2_filtered2 = my_table2.filter(lambda x: float(x["ranking"]) >= 10)
for item2 in my_table2_filtered2.table:
    game2.append(float(item2['games']))
print(game1)
print(game2)
avg_game1 = sum(game1)/len(game1)
avg_game2 = sum(game2)/len(game2)
print(avg_game1)
print(avg_game2)

passes_comparison = []
passes_comparison2 = []
my_table3_filtered = my_table1.filter(lambda x: x["position"] == "midfielder")
my_table3_filtered2 = my_table1.filter(lambda x: x["position"] == "forward")
for item3 in my_table3_filtered.table:
    passes_comparison.append(float(item3["passes"]))

for item4 in my_table3_filtered2.table:
    passes_comparison2.append(float(item4["passes"]))
avg_pass1 = sum(passes_comparison)/len(passes_comparison)
avg_pass2 = sum(passes_comparison2)/len(passes_comparison2)
print(avg_pass1)
print(avg_pass2)