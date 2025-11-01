import typer
import logging
from pathlib import Path
import yaml
from jinja2 import Environment, FileSystemLoader
import subprocess
import time
import socket
from typing import Optional, List

# 애플리케이션 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stack-forge")

app = typer.Typer(
    help="Stack Forge: Docker 기반의 로컬 개발 환경을 손쉽게 구축하고 관리합니다."
)

GENERATED_FILE = ".stack_forge.generated.yaml"
CONFIG_FILE = "stack_forge.yaml"


@app.command()
def status():
    """
    현재 실행 중인 스택의 상태와 접속 정보를 보여줍니다.
    """
    logger.info("스택 상태를 확인합니다...")
    config_file = Path(GENERATED_FILE)
    if not config_file.exists():
        logger.warning(
            f"'{GENERATED_FILE}' 파일을 찾을 수 없습니다. 'stack-forge up'을 먼저 실행하세요."
        )
        return

    subprocess.run(["docker-compose", "-f", str(config_file), "ps"], check=True)

    try:
        with open(CONFIG_FILE) as f:
            config = yaml.safe_load(f)
        services = config.get("services", {})
        service_config = config.get("service_config", {})

        logger.info("--- 서비스 접속 정보 ---")
        if services.get("postgres"):
            port = service_config.get("postgres", {}).get("port", 5432)
            user = service_config.get("postgres", {}).get("user", "admin")
            pw = service_config.get("postgres", {}).get("password", "password")
            db = service_config.get("postgres", {}).get("db_name", "stack_forge_db")
            logger.info(f"Postgres: postgresql://{user}:{pw}@localhost:{port}/{db}")

        if services.get("airbyte"):
            port = service_config.get("airbyte", {}).get("port", 8000)
            logger.info(f"Airbyte UI: http://localhost:{port}")

        if services.get("dbt"):
            logger.info(
                "dbt: 'stack-forge run dbt ...' 또는 'stack-forge shell dbt' 명령어로 사용 가능합니다."
            )

    except FileNotFoundError:
        logger.warning(
            f"'{CONFIG_FILE}' 파일을 찾을 수 없습니다. 접속 정보를 표시할 수 없습니다."
        )
    except Exception as e:
        logger.error(f"설정 파일을 읽는 중 오류가 발생했습니다: {e}")


@app.command()
def logs(
    service_name: Optional[str] = typer.Argument(None, help="로그를 확인할 서비스 이름"),
    follow: bool = typer.Option(
        False, "-f", "--follow", help="실시간으로 로그를 스트리밍합니다."
    ),
):
    """
    지정된 서비스의 로그를 출력합니다.
    """
    config_file = Path(GENERATED_FILE)
    if not config_file.exists():
        logger.warning(
            f"'{GENERATED_FILE}' 파일을 찾을 수 없습니다. 'stack-forge up'을 먼저 실행하세요."
        )
        return

    command = ["docker-compose", "-f", str(config_file), "logs"]
    if follow:
        command.append("-f")
    if service_name:
        command.append(service_name)
        logger.info(f"'{service_name}' 서비스의 로그를 스트리밍합니다... (Ctrl+C to stop)")
    else:
        logger.info("모든 서비스의 로그를 출력합니다...")

    try:
        subprocess.run(command)
    except KeyboardInterrupt:
        logger.info("로그 스트리밍을 중단합니다.")


@app.command()
def shell(
    service_name: str = typer.Argument(
        ..., help="접속할 서비스의 이름 (예: dbt, postgres)"
    )
):
    """
    실행 중인 서비스의 컨테이너 셸에 접속합니다. (예: stack-forge shell dbt)
    """
    config_file = Path(GENERATED_FILE)
    if not config_file.exists():
        logger.warning(
            f"'{GENERATED_FILE}' 파일을 찾을 수 없습니다. 'stack-forge up'을 먼저 실행하세요."
        )
        return

    logger.info(f"'{service_name}' 서비스의 셸에 접속합니다...")

    try:
        subprocess.run(
            ["docker-compose", "-f", str(config_file), "exec", service_name, "bash"],
            check=True,
        )
    except subprocess.CalledProcessError:
        logger.warning("'bash'를 찾을 수 없어 'sh'로 다시 시도합니다...")
        try:
            subprocess.run(
                ["docker-compose", "-f", str(config_file), "exec", service_name, "sh"],
                check=True,
            )
        except Exception as e:
            logger.error(f"'{service_name}' 셸 접속에 실패했습니다: {e}")
            logger.error("서비스가 실행 중인지 확인하세요. 'stack-forge status' 명령어로 확인할 수 있습니다.")


@app.command(help="실행 중인 서비스 컨테이너 내부에서 명령어를 실행합니다.")
def run(
    service_name: str = typer.Argument(..., help="명령어를 실행할 서비스 이름"),
    command: List[str] = typer.Argument(..., help="서비스에서 실행할 명령어"),
):
    logger.info(f"'{service_name}'에서 다음 명령어를 실행합니다: {' '.join(command)}")
    config_file = Path(GENERATED_FILE)
    if not config_file.exists():
        logger.warning(
            f"'{GENERATED_FILE}' 파일을 찾을 수 없습니다. 'stack-forge up'을 먼저 실행하세요."
        )
        return

    command_list = [
        "docker-compose",
        "-f",
        str(config_file),
        "exec",
        service_name,
        *command,
    ]

    try:
        subprocess.run(command_list, check=True)
    except Exception as e:
        logger.error(f"'{service_name}'에서 명령어 실행에 실패했습니다: {e}")


@app.command()
def up():
    """
    `stack_forge.yaml`에 정의된 모든 서비스를 시작하고 준비될 때까지 대기합니다.
    """
    try:
        with open(CONFIG_FILE) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(
            f"'{CONFIG_FILE}' 파일을 찾을 수 없습니다. 'stack-forge init'을 먼저 실행하세요."
        )
        raise typer.Exit(code=1)

    services = config.get("services", {})
    services_config = config.get("service_config", {})
    dbt_config = config.get("dbt", {})

    # Docker-compose 파일 생성
    templates_path = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(templates_path))
    template = env.get_template("docker-compose.yaml.j2")

    rendered = template.render(
        services=services, service_config=services_config, dbt_config=dbt_config
    )

    output_path = Path(GENERATED_FILE)
    output_path.write_text(rendered)
    logger.info(f"Docker-compose 설정 파일 생성: '{output_path}'")

    # 서비스 시작
    active_services = [s for s, e in services.items() if e]
    if not active_services:
        logger.warning("활성화된 서비스가 없습니다. 'stack_forge.yaml' 파일을 확인하세요.")
        return

    logger.info(f"서비스를 시작합니다: {', '.join(active_services)}")
    try:
        subprocess.run(
            ["docker-compose", "-f", str(output_path), "up", "-d"], check=True
        )
    except Exception as e:
        logger.error(f"서비스 시작에 실패했습니다: {e}")
        logger.error("Docker가 실행 중인지 확인하세요.")
        raise typer.Exit(code=1)

    # 서비스 준비 대기
    logger.info("서비스가 준비될 때까지 대기합니다...")
    try:
        if services.get("postgres"):
            pg_port = services_config.get("postgres", {}).get("port", 5432)
            _wait_for_port("localhost", pg_port, timeout=60)
            logger.info("✅ Postgres가 준비되었습니다.")

        if services.get("airbyte"):
            ab_port = services_config.get("airbyte", {}).get("port", 8000)
            _wait_for_port("localhost", ab_port, timeout=120)
            logger.info("✅ Airbyte가 준비되었습니다.")

    except TimeoutError as e:
        logger.error(f"서비스 준비 확인에 실패했습니다: {e}")
        logger.error("'stack-forge logs [서비스 이름]' 명령어로 로그를 확인하세요.")
        raise typer.Exit(code=1)

    logger.info("🎉 모든 서비스가 성공적으로 시작되었습니다.")
    status()


def _wait_for_port(host: str, port: int, timeout: int = 60):
    """지정된 포트가 열릴 때까지 대기합니다."""
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except (ConnectionRefusedError, socket.timeout, OSError):
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"{host}:{port} 포트가 {timeout}초 내에 준비되지 않았습니다."
                )
            time.sleep(2)


@app.command()
def down(
    clean: bool = typer.Option(
        False, "-v", "--volumes", "--clean", help="Docker 볼륨(데이터)을 함께 삭제합니다."
    )
):
    """
    실행 중인 모든 서비스를 중지합니다.
    """
    logger.info("스택을 종료합니다...")
    config_file = Path(GENERATED_FILE)
    if not config_file.exists():
        logger.warning(f"'{GENERATED_FILE}'을 찾을 수 없습니다. 종료할 스택이 없습니다.")
        return

    command = ["docker-compose", "-f", str(config_file), "down"]
    if clean:
        command.append("-v")
        logger.info("데이터 볼륨을 함께 삭제합니다...")

    try:
        subprocess.run(command, check=True)
        logger.info("✅ 스택이 성공적으로 종료되었습니다.")
    except Exception as e:
        logger.error(f"스택 종료에 실패했습니다: {e}")


@app.command()
def init():
    """
    현재 디렉토리에 `stack_forge.yaml` 기본 설정 파일을 생성합니다.
    """
    logger.info("Stack Forge 초기화를 시작합니다...")
    config_file = Path(CONFIG_FILE)

    DEFAULT_YAML_CONTENT = """# Stack Forge 기본 설정 파일
# 'services' 아래에 서비스를 추가하여 활성화할 수 있습니다.
# 각 서비스의 상세 설정은 'service_config'에서 변경할 수 있습니다.

version: "1.0"

# 사용할 서비스를 주석 해제하여 활성화하세요.
services:
  postgres: {}
  dbt: {}
  airbyte: {}

# -------------------------------------------------------------------
# 서비스별 상세 설정
# -------------------------------------------------------------------
service_config:
  postgres:
    db_name: "stack_forge_db"
    user: "admin"
    password: "password"
    port: 5432
  airbyte:
    port: 8000

# -------------------------------------------------------------------
# dbt 프로젝트 연동 설정
# -------------------------------------------------------------------
dbt:
  project_dir: "./dbt_project" # dbt 프로젝트가 있는 로컬 경로
"""

    if config_file.exists():
        if typer.confirm(f"'{CONFIG_FILE}' 파일이 이미 존재합니다. 덮어쓰시겠습니까?"):
            config_file.write_text(DEFAULT_YAML_CONTENT.strip())
            logger.info(f"'{CONFIG_FILE}' 파일을 덮어썼습니다.")
        else:
            logger.info("초기화를 취소했습니다.")
    else:
        config_file.write_text(DEFAULT_YAML_CONTENT.strip())
        logger.info(f"기본 설정 파일 '{CONFIG_FILE}'을 생성했습니다.")


if __name__ == "__main__":
    app()
