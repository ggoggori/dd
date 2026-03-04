import logging
import sys
import os

# 1. 로거 이름 정의 (프로젝트 이름을 사용)
LOGGER_NAME = 'VoltaFlow_Pipeline'

# 2. 로그 파일 경로 설정
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, 'pipeline_activity.log')


def setup_logging():
    """
    프로젝트 전체에서 사용할 로깅 핸들러와 포맷을 설정합니다.
    """
    
    # 루트 로거 또는 특정 로거 이름으로 가져옵니다.
    logger = logging.getLogger(LOGGER_NAME)
    
    # 중복 설정 방지 (이미 설정된 경우 건너뜁니다)
    if logger.hasHandlers():
        return logger

    # 최종 출력 레벨을 INFO 이상으로 설정 (DEBUG 메시지는 필터링됩니다)
    logger.setLevel(logging.INFO) 
    
    # 로그 포맷 정의: 시간, 레벨, 파일명, 함수명, 메시지
    formatter = logging.Formatter(
        '%(asctime)s - [%(levelname)s] - %(filename)s:%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 3. 콘솔(스트림) 핸들러 설정
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO) # 콘솔에는 INFO 이상만 출력

    # 4. 파일 핸들러 설정
    # FileHandler 대신 TimedRotatingFileHandler를 사용하면 날짜별로 파일이 자동 분리되어 좋습니다.
    file_handler = logging.FileHandler(LOG_FILE_PATH, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG) # 파일에는 모든 DEBUG 메시지를 기록

    # 5. 로거에 핸들러 추가
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    
    return logger

# 프로젝트 전체에서 사용할 로거 인스턴스를 바로 노출
logger = setup_logging()

# 예시: 로그 사용법
# logger.info("로깅 설정 완료!")