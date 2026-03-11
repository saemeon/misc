import inspect


def search_object_in_stack(class_to_find: type, variable_names: str | list[str]):
    """
    Search for an object of a specific class type in the call stack based on a list of variable names.

    Parameters
    ----------
    class_to_find : type
        The class type to search for (e.g., `SparkSession`).
    variable_names : list of str
        A list of variable names (strings) to look for in the call stack.

    Returns
    -------
    object or None
        The found object of the specified class type if found in the call stack,
        otherwise `None` if no object is found.

    Notes
    -----
    This function traverses the call stack and searches each frame's global variables
    for the specified variable names. If an object of the desired class type is found,
    it is returned. Otherwise, the function returns `None`.
    """
    frame = inspect.currentframe()
    while frame is not None:
        for name in list(variable_names):
            # Check if the variable name exists in the global variables of the current frame
            if name in frame.f_globals and isinstance(
                frame.f_globals[name], class_to_find
            ):
                found_object = frame.f_globals[name]
                print(f"{class_to_find.__name__} found in variable '{name}'.")
                return found_object
        frame = frame.f_back

    print(f"No {class_to_find.__name__} found in the call stack.")
    return None



    from pathlib import PurePosixPath
from pyspark.sql import SparkSession


class LoadablePath(PurePosixPath):
    def load(self, spark_session=None):
        if spark_session is None:
            spark_session = search_object_in_stack(
                SparkSession, ["spark_session", "sc"]
            )
        assert spark_session is not None
        return snb_nets.spark_read_parquet(spark_session, self)


LoadablePath(DATALAKE.group_project_ads / DATALAKE.nodelist.name).load().printSchema()



import inspect
from pyspark.sql import SparkSession


# Define a custom class with a load method
class CustomClass:
    def __init__(self):
        self.spark_session = None

    def load(self):
        # Traverse the call stack
        frame = inspect.currentframe()
        while frame is not None:
            # Check if the frame has a SparkSession object
            if "spark" in frame.f_globals and isinstance(
                frame.f_globals["spark"], SparkSession
            ):
                self.spark_session = frame.f_globals["spark"]
                print("SparkSession found!")
                break
            frame = frame.f_back

        if self.spark_session is None:
            print("No SparkSession found in the call stack.")
        else:
            # Perform any action with the SparkSession if found
            print("Using SparkSession:", self.spark_session)


# Example usage
def example_function():
    obj = CustomClass()
    obj.load()  # This should find the `spark` session from the call stack


spark = sc
example_function()
