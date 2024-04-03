class Variation:
    """
    Variation helps registering variations for columns and query them

    Every column can be represented by a str or tuple

    Every variation must have a unique name (e.g. ``JES_up``)
    """

    def __init__(self):
        # variations_dict for each variation will save a list of substitutions to perform
        # each substitution is registered as a tuple (nominal_column, varied_column)
        self.variations_dict = {}
        # columns_dict for each column will save a list of all variations that affect such column
        self.columns_dict = {}

    @staticmethod
    def format_varied_column(variation_name, column):
        if isinstance(column, str):
            return f"{column}_{variation_name}"
        elif isinstance(column, tuple):
            _list = list(column[:-1])
            _list.append(f"{column[-1]}_{variation_name}")
            return tuple(_list)
        else:
            print(
                "Cannot format varied column", column, "for variation", variation_name
            )
            raise Exception

    def create_variation(self, variation_name, columns):
        """
        Register a new variation with name ``variation_name`` for the ``columns`` provided

        Parameters
        ----------
        variation_name : str
            Unique string to identify this variation
        columns : list of str or list of tuple
            The list of all columns that are affected by this variation
        """
        self.variations_dict[variation_name] = []
        self.add_columns_for_variation(variation_name, columns)

    def add_columns_for_variation(self, variation_name, columns):
        for column in columns:
            self.variations_dict[variation_name].append(
                (
                    column,
                    Variation.format_varied_column(variation_name, column),
                )
            )
            variation_list = self.columns_dict.get(column, [])
            variation_list.append(variation_name)
            self.columns_dict[column] = variation_list

    def get_variations_column(self, column, pattern="all"):
        if pattern == "all":
            return self.columns_dict[column]

    def get_variations_all(self):
        return list(self.variations_dict.keys())

    def get_variation_subs(self, variation_name):
        return self.variations_dict[variation_name]
