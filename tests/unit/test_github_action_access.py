from gcsfs import GCSFileSystem


def test(token):
    with open("cred.json", "w") as ofile:
        ofile.write(token)
    fs = GCSFileSystem(project="sandbox-281209", token="cred.json")
    print(list(fs.ls("code_tests_sand")))
