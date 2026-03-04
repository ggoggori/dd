from .PNE import CycProcessor, CtsProcessor
from .APRO import AproProcessor

class FactoryProcessor():
    ext_map = {
        'cts' : CtsProcessor,
        'cyc' : CycProcessor,
        'dat' : AproProcessor
    }
    
    @staticmethod
    def create_processor(file_path, config):
        ext = file_path.split('.')[-1]
        if ext in FactoryProcessor.ext_map:
            return FactoryProcessor.ext_map[ext](config)
        else:
            raise NotImplementedError(f"Processor for {ext} is not implemented")