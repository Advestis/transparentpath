from typing import Dict, Tuple, List


class MethodTranslator(object):
    """ """

    def __init__(
        self, first_name: str, second_name: str, kwarg_names: Dict[str, str] = None,
    ):

        self.first_name = first_name
        self.second_name = second_name
        self.kwargs = {key: kwarg_names[key] for key in kwarg_names if kwarg_names[key] != ""}
        if self.kwargs is None:
            self.kwargs = {}

    def translate_str(self, *args: Tuple, **kwargs: Dict) -> str:
        """ Tranlates the method as a string

        Parameters
        ----------
        *args: Tuple
        **kwargs: Dict

        Returns
        -------
        str
            The string of the translated method
            new_method(arg1, arg2..., kwargs1=val1, translated_kwargs2=val2...)
        """

        tr = f"{self.second_name}("
        for val in args:
            tr = f"{tr}{val}, "
        for kwarg in kwargs:
            val = kwargs[kwarg]
            if isinstance(val, str):
                val = f"'{val}'"
            if kwarg in self.kwargs:
                tr = f"{tr}{self.kwargs[kwarg]}={val}, "
            else:
                tr = f"{tr}{kwarg}={val}, "
        if len(tr) > 2 and tr[-2:] == ", ":
            tr = tr[:-2]

        tr = f"{tr})"
        return tr

    def translate(self, *args: Tuple, **kwargs: Dict) -> [str, Tuple, Dict]:
        """ Translates the method

        Parameters
        ----------
        *args: Tuple
        **kwargs: Dict

        Returns
        -------
        [str, Tuple, Dict]
            The translated method name along with the given args and the translated kwargs
        """
        new_kwargs = {}
        for kwarg in kwargs:
            val = kwargs[kwarg]
            if kwarg in self.kwargs:
                new_kwargs[self.kwargs[kwarg]] = val
            else:
                new_kwargs[kwarg] = val
        return self.second_name, args, new_kwargs


class MultiMethodTranslator(object):
    """ """

    def __init__(
        self, first_name: str, cases: List[str], second_names: List[str], kwargs_names: [List[Dict[str, str]]] = None,
    ):

        self.first_name = first_name
        self.cases = cases
        self.second_names = second_names
        self.kwargs = kwargs_names
        self.translators = {}
        if self.kwargs is None:
            self.kwargs = [{}] * len(self.cases)

        for case, second_name, kwarg_names in zip(self.cases, self.second_names, self.kwargs):
            self.translators[case] = MethodTranslator(self.first_name, second_name, kwarg_names)

    def translate_str(self, case: str, *args: Tuple, **kwargs: Dict) -> str:
        """ Tranlates the method as a string according to a case

        Parameters
        ----------
        case: str
            The name of the translation case to use
        *args: Tuple
        **kwargs: Dict

        Returns
        -------
        str:
            The string of the translated method
            new_method(arg1, arg2..., kwargs1=val1, translated_kwargs2=val2...)
        """
        if case not in self.cases:
            raise ValueError(f"Case {case} not fonud in object")
        return self.translators[case].translate_str(*args, **kwargs)

    def translate(self, case: str, *args: Tuple, **kwargs: Dict) -> [str, Tuple, Dict]:
        """ Translates the method according to a case

        Parameters
        ----------
        case: str
            The name of the translation case to use
        *args: Tuple
        **kwargs: Dict

        Returns
        -------
        [str, Tuple, Dict]
            The translated method name along with the given args and the
            translated kwargs
        """
        if case not in self.cases:
            raise ValueError(f"Case {case} not fonud in object")
        return self.translators[case].translate(*args, **kwargs)
