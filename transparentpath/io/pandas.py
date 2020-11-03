
class MyHDFStore(pd.HDFStore):
    """Same as MyHDFFile but for pd.HDFStore objects"""

    def __init__(self, *args, remote: Union[TransparentPath, None] = None, **kwargs):
        self.remote_file = remote
        self.local_path = args[0]
        # noinspection PyUnresolvedReferences,PyProtectedMember
        if isinstance(self.local_path, tempfile._TemporaryFileWrapper):
            pd.HDFStore.__init__(self, args[0].name, *args[1:], **kwargs)
        else:
            pd.HDFStore.__init__(self, *args, **kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        pd.HDFStore.__exit__(self, exc_type, exc_value, traceback)
        if self.remote_file is not None:
            TransparentPath(self.local_path.name, fs="local").put(self.remote_file)
            self.local_path.close()