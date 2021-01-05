from gcsfs import GCSFileSystem

fs = GCSFileSystem(project="sandbox-281209")

print(list(fs.ls("code_tests_sand")))
