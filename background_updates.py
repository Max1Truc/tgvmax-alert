import logging, os

import db

def mainloop():
    logging.basicConfig(level=logging.INFO)
    os.makedirs("./.data/public/incremental/", exist_ok=True)
    db.update_loop()

if __name__ == '__main__':
    mainloop()
