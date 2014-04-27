
if __name__ == "__main__":
    import logging
    logger = logging.getLogger('myapp')
    hdlr = logging.FileHandler('myapp.log')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)

    logger.error('Terrible problem.')
    logger.info('This is just chatty')
