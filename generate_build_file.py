import os

LOAD_TEMPLATE = """
load("@aspect_rules_py//py:defs.bzl", "py_library", "py_test")
"""

# py_library 템플릿 정의
LIBRARY_TEMPLATE = """
py_library(
    name = "{lib_name}",
    srcs = ["{src_file}"],
    visibility = ["//visibility:public"],
)
"""

def create_build_file(directory):
    # 디렉토리 내의 모든 .py 파일에 대해 py_library 생성
    build_content = LOAD_TEMPLATE
    for filename in os.listdir(directory):
        if filename.endswith(".py") and filename != "__init__.py":
            lib_name = filename[:-3]  # 파일명에서 .py를 제거
            build_content += LIBRARY_TEMPLATE.format(lib_name=lib_name, src_file=filename)

    # BUILD.bazel 파일 생성
    if build_content:
        build_path = os.path.join(directory, "BUILD.bazel")
        with open(build_path, "w") as build_file:
            build_file.write(build_content)

def main():
    # 현재 디렉토리부터 시작하여 모든 하위 디렉토리를 순회
    for root, dirs, files in os.walk("src/tunip"):
        create_build_file(root)

if __name__ == "__main__":
    main()
