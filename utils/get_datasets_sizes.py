import os

FILE_SORGENTE = "../datasets/bookcorpus.csv" 

#i'm creating 3 files with different sizes
LIMITI = [
    ("book_0.5GB.txt", 0.5 * 1024**3),
    ("book_1.0GB.txt", 1.0 * 1024**3),
    ("book_1.5GB.txt", 1.5 * 1024**3)
]

def crea_versioni():
    outputs = [(open(nome, "w", encoding="utf-8"), limite) for nome, limite in LIMITI]
    
    scritto = 0 #it works as a counter of bytes written
    max_target = LIMITI[-1][1] #it works as stop limit

    print("Elaborazione in corso...")
    
    with open(FILE_SORGENTE, "r", encoding="utf-8", errors="ignore") as f_in:
        for riga in f_in:
            riga_completa = riga.strip() + "\n"
            peso_riga = len(riga_completa.encode("utf-8"))
            
            for f_out, limite in outputs: #it writes in each file if it has not reached its limit
                if scritto < limite:
                    f_out.write(riga_completa)
            
            scritto += peso_riga
            
            if scritto >= max_target:
                break

    for f_out, _ in outputs: #good practice closing files previously opened
        f_out.close()
    
    print(f"Done!")

if __name__ == "__main__":
    if os.path.exists(FILE_SORGENTE):
        crea_versioni()
    else:
        print(f"Error: The file {FILE_SORGENTE} was not found.")