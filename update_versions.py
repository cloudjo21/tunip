import pkg_resources

# 현재 설치된 패키지 목록 가져오기
installed_packages = {pkg.key: pkg.version for pkg in pkg_resources.working_set}

# requirements.txt 파일 읽기
with open("requirements.txt", "r") as f:
    lines = f.readlines()

# 업데이트된 requirements 리스트 생성
updated_requirements = []
for line in lines:
    package = line.strip()
    if package and "==" not in package:  # 버전이 없는 경우
        version = installed_packages.get(package.lower())  # 설치된 버전 가져오기
        if version:
            updated_requirements.append(f"{package}=={version}\n")
        else:
            updated_requirements.append(f"{package}\n")  # 설치되지 않은 경우 그대로 유지
    else:
        updated_requirements.append(f"{package}\n")

# 업데이트된 내용을 새로운 requirements.txt 파일에 저장
with open("requirements_updated.txt", "w") as f:
    f.writelines(updated_requirements)

print("Updated requirements saved to requirements_updated.txt")
