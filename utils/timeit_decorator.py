from datetime import datetime


def timeit_decorator(logger):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = datetime.now()
            logger.info(f"Start {func.__name__}")
            result = func(*args, **kwargs)
            end = datetime.now()
            logger.info(f"Elapsed time for {func.__name__}: {end - start}")
            return result

        return wrapper

    return decorator
