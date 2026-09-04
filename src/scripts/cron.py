import logging
from scripts.generating_lake import main as generating_lake
from scripts.bronze import main as bronze
from scripts.silver import main as silver
from scripts.gold import main as gold

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Ce script lance les différentes étapes du pattern médaillon : génération du data lake, bronze, silver, gold")

def main():
    try:
        logging.info('Génération du data lake \n')
        generating_lake()
        logging.info('\n Fin de la génération du data lake \n')

        logging.info('Lancement de l\'étape Bronze \n')
        bronze()
        logging.info('\n Fin de l\'étape Bronze \n')

        logging.info('Lancement de l\'étape Silver \n')
        silver()
        logging.info('\n Fin de l\'étape Silver \n')

        logging.info('Lancement de l\'étape Gold \n')
        gold()
        logging.info('\n Fin de l\'étape Gold \n')
    except Exception as e:
        raise e


if __name__ == "__main__":
    main()