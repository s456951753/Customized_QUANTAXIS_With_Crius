import Utils.configuration_file_service as config_service
import tushare as ts

class tushare_interface:
    '''
    Get a singleton of tushare's pro interface. If token is passed in as a not-None value, it will be used as the token
    to call tushare api. Otherwise the token configured in configuration file will be used.
    '''
    _pro = None

    def __new__(cls, token=None):
        if(cls._pro is None):
            if(token is None):
                token = config_service.getProperty(section_name=config_service.TOKEN_SECTION_NAME,
                                               property_name=config_service.TS_TOKEN_NAME)
            cls._pro = ts.pro_api(token)
            return cls._pro
        else:
            return cls._pro
