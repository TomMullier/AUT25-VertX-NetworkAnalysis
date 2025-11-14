import csv
import os

# Dossier contenant les fichiers CSV
csv_folder = "csv"

# Charger les fichiers CSV
csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

if len(csv_files) != 2:
        raise ValueError("Le dossier doit contenir exactement deux fichiers CSV pour la comparaison.")

csv_file_1 = os.path.join(csv_folder, csv_files[0])
csv_file_2 = os.path.join(csv_folder, csv_files[1])

def load_csv(file_path):
        with open(file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                return [list(map(lambda x: x if x.strip() else '0', row)) for row in reader]

# Charger les données des deux fichiers
data_1 = load_csv(csv_file_1)
data_2 = load_csv(csv_file_2)

line_number = 0
different_lines = 0

# Vérifier si les dimensions des fichiers sont identiques
if len(data_1) != len(data_2):
        print("Les fichiers CSV ont un nombre de lignes différent.")
else:
        line_number = len(data_1)
        different_lines = 0
        for i, (row1, row2) in enumerate(zip(data_1, data_2)):
                if len(row1) != len(row2):
                        print(f"Différence détectée à la ligne {i + 1}: les colonnes ne correspondent pas.")
                else:
                        differences = [(j, val1, val2) for j, (val1, val2) in enumerate(zip(row1, row2)) if val1 != val2]
                        if differences:
                                print(f"Différences à la ligne {i + 1}:")
                                different_lines += 1
                                for col, val1, val2 in differences:
                                        print(f"  Colonne {col + 1}: {val1} != {val2}")
                        
                                
print(f"\nTotal des lignes comparées: {line_number}")
print(f"Total des lignes différentes: {different_lines}")
if different_lines == 0:
        print("Les deux fichiers CSV sont identiques.")
else:
        print("Les deux fichiers CSV présentent des différences.")
        # % accuracy
        
accuracy = ((line_number - different_lines) / line_number) * 100
print(f"Exactitude de la comparaison: {accuracy:.2f}%")
        