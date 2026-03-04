from utils import utils
from processor.Factory import FactoryProcessor
from assets import config

if __name__ == '__main__':
    paths = utils.find_files("./data")
    print("There are", len(paths), "files to process")
    
    for path in paths:
        if '.cyc' in str(path): #아직은 stepend만 사용하기 때문에 cyc는 사용하지 않는다.
            continue
        print(path)
        processor = FactoryProcessor.create_processor(str(path), config)
        stepend_df = processor.parse_binary_file(path)['StepEndData']
        stepend_df.to_csv('results/'+ path.parts[-1] + '.csv', index=False, encoding='cp949')
        
    print("All files are processed")
        