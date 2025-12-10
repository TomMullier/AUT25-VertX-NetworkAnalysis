import csv
import os
import ipaddress

print("Comparaison des fichiers CSV des paquets, générés par la plateforme et Scapy")

# Fonction demandée : normalisation IPv6 en format fully exploded
def normalize_ip(ip):
    try:
        addr = ipaddress.ip_address(ip)
        if addr.version == 6:
            hextets = addr.exploded.split(":")
            return ":".join(hextets)
        else:
            return str(addr)
    except ValueError:
        return ip


# Dossier contenant les fichiers CSV
csv_folder = "csv"

# Noms des fichiers CSV à comparer
csv_file_names = [
    "plateform_output_benign_slowloris.csv",
    "plateform_output_friday.csv",
    "plateform_output_reference.csv",
    "scapy_output_benign_slowloris.csv",
    "scapy_output_friday.csv",
    "scapy_output_reference.csv",
]

# Charger les fichiers CSV
csv_files = [f for f in os.listdir(csv_folder) if f.endswith(".csv")]

csv_file_1 = os.path.join(csv_folder, csv_file_names[0])
csv_file_2 = os.path.join(csv_folder, csv_file_names[3])
csv_file_5 = os.path.join(csv_folder, csv_file_names[2])
csv_file_6 = os.path.join(csv_folder, csv_file_names[5])


def load_csv(file_path):
    with open(file_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        rows = [list(map(lambda x: x if x.strip() else "0", row)) for row in reader]

        # Normalisation IPv6 sur colonnes 2 et 3 (index 1 et 2)
        for row in rows:
            if len(row) >= 3:
                row[1] = normalize_ip(row[1])
                row[2] = normalize_ip(row[2])

        return rows


# Charger les données des fichiers
data_1 = load_csv(csv_file_1)
data_2 = load_csv(csv_file_2)
data_5 = load_csv(csv_file_5)
data_6 = load_csv(csv_file_6)


def compare_csv(data_a, data_b, label_a, label_b):
    line_number = 0
    different_lines = 0

    if len(data_a) != len(data_b):
        print(f"Les fichiers {label_a} et {label_b} ont un nombre de lignes différent.")
        return

    for i, (row_a, row_b) in enumerate(zip(data_a, data_b)):
        if len(row_a) != len(row_b):
            print(
                f"Différence détectée à la ligne {i + 1} entre {label_a} et {label_b}: les colonnes ne correspondent pas."
            )
        else:
            differences = [
                (j, val_a, val_b)
                for j, (val_a, val_b) in enumerate(zip(row_a, row_b))
                if val_a != val_b
            ]
            if differences:
                print(f"Différences à la ligne {i + 1} entre {label_a} et {label_b}:")
                different_lines += 1
                for col, val_a, val_b in differences:
                    print(f"  Colonne {col + 1}: {val_a} != {val_b}")
        line_number += 1

    print(f"Comparaison terminée entre {label_a} et {label_b}.")
    print(f"\nTotal des lignes comparées: {line_number}")
    print(f"Total des lignes différentes: {different_lines}\n")

    if different_lines == 0:
        print("Les deux fichiers CSV sont identiques.")
    else:
        print("Les deux fichiers CSV présentent des différences.")

    accuracy = ((line_number - different_lines) / line_number) * 100
    print(f"Exactitude de la comparaison: {accuracy:.2f}%")


# Comparer les fichiers CSV
compare_csv(data_1, data_2, csv_file_names[0], csv_file_names[3])
compare_csv(data_5, data_6, csv_file_names[2], csv_file_names[5])
