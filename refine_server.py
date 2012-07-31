from requests import get as http_get, post as http_post, codes as http_codes, exceptions as http_exceptions
from urllib import quote_plus
from random import randint
try: import simplejson as json
except ImportError: import json
from os.path import basename
from time import sleep
from mimetools import choose_boundary

class refine_format():

    def __init__(self, id=None, name=None, label=None, download=None, uiClass=None, *args, **kwargs):
        self.id = kwargs.get('id', id)
        self.name = kwargs.get('name', name)
        self.label = kwargs.get('label', label)
        self.download = kwargs.get('download', download)
        self.uiClass = kwargs.get('uiClass', uiClass)


class refine_configuration():

    def __init__(self, **kwargs):
        if kwargs:
            self.formats = (refine_format(name=k, **kwargs['formats'][k]) for k in kwargs['formats'].keys())
            self.mime_types = kwargs['mimeTypeToFormat']
            self.file_extensions = kwargs['extensionToFormat']
        else:
            self.formats = None
            self.mime_types = None
            self.file_extensions = None

class refine_version():

    def __init__(self, full_name=None, full_version=None, revision=None, version=None, *args, **kwargs):
        self.full_name = kwargs.get('full_name',full_name)
        self.full_version = kwargs.get('full_version', full_version)
        self.revision = kwargs.get('revision', revision)
        self.version = kwargs.get('version', version)

class refine_server():

    def __init__(self, protocol='http', host='127.0.0.1', port='3333', *args, **kwargs):
        self.protocol = kwargs.get('protocol', protocol)
        self.host = kwargs.get('host', host)
        self.port = kwargs.get('port', port)

    def get(self, action):
        try: return http_get("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action))
        except http_exceptions.RequestException:
            raise

    def post(self, action, data=None, headers=None, files=None, **kwargs):
        try:
            new_kwargs = {"data":kwargs.get("data",data),"files":kwargs.get("files",files), "headers":kwargs.get("headers",headers)}
            response = http_post("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action), **new_kwargs)
            print "REQUEST URL : " + str(response.request.url) + "\nDATA : " + str(response.request.data) + "\nHEADERS : " + str(response.request.headers)
            print "RESPONSE : " + response.text
            return response
        except http_exceptions.RequestException: print "Request {0} failed.".format(action)

    @property
    def version(self):
        response = self.get("command/core/get-version")
        if response.status_code == http_codes.ok: return refine_version(**response.json)

    @property
    def projects(self):
            response = self.get("command/core/get-all-project-metadata")
            if response.status_code == http_codes.ok:
                return (project(id = pid) for pid in response.json['projects'].keys())
            else: print "Request command/core/get-all-project-metadata failed."

    @property
    def configuration(self):
            response = self.post("command/core/get-importing-configuration")
            if response.status_code == http_codes.ok:
                return refine_configuration(**response.json['config'])
            else: print "Request command/core/get-importing-configuration failed."

class data_source():

    def __init__(self, declaredMimeType=None, location=None, fileName=None, origin=None, url=None, size=None, *args, **kwargs):
        self.declaredMimeType=kwargs.get('declaredMimeType', declaredMimeType)
        self.location = kwargs.get('location', location)
        self.fileName = kwargs.get('fileName', fileName)
        self.origin = kwargs.get('origin', origin)
        self.url = kwargs.get('url', url)
        self.size = kwargs.get('size', size)

class retrieval_record():

    def __init__(self, files=None, downloadCount=None, archiveCount=None, clipboardCount=None, uploadCount=None, *args, **kwargs):
        self.files = (data_source(**f) for f in kwargs.get('files', files))
        self.downloadCount = kwargs.get('downloadCount', downloadCount)
        self.archiveCount = kwargs.get('archiveCount', archiveCount)
        self.clipboardCount = kwargs.get('clipboardCount', clipboardCount)
        self.uploadCount = kwargs.get('uploadCount', uploadCount)

class import_job_details():

    def __init__(self, rankedFormats=None, hasData=None, state=None, fileSelection=None, retrievalRecord=None, *args, **kwargs):
        self.rankedFormats = kwargs.get('rankedFormats', rankedFormats) # array of mime types in order of best guess for this data source
        self.hasData = kwargs.get('hasData', hasData) # boolean
        self.state = kwargs.get('state', state) # "ready"
        self.fileSelection = kwargs.get('fileSelection', fileSelection) # [0]  ...what does that mean? is it an array of indices that correspond to values in retrievalRecord["files"]?
        if retrievalRecord or "retrievalRecord" in kwargs: self.retrievalRecord = retrieval_record(**kwargs.get('retrievalRecord', retrievalRecord)) # another object...
        else: self.retrievalRecord = None

class project():

    def __init__(self, server=refine_server(), id=None, path=None, url=None, name=None, *args, **kwargs):
    # input kwargs are passed on to __update_formats where they are used to determine the first object in an XML/JSON/RDF doc, the encoding, the separator for text files (.*sv), and the column widths (and optionally headings) for fixed width input
    # recordPath (required for XML, JSON, line-based and RDF) e.g. ["__anonymous__","media","__anonymous__"] where __anonymous__ is what refine names a nameless value/object
    # encoding (optional for RDF and line-based) e.g. any valid encoding e.g. "UTF8"
    # separator (optional, defaults to tab, line-based and *sv) e.g. "\\t" or "," - any string acceptable
    # columnWidths (required for fixed-width) i.e. a list of character widths
    # columnNames (optional for fixed width) i.e. a list
        self.server = server
        self.id = kwargs.get('id', id)
        if not self.id:
            job_id = self.__fetch_new_job()
            if path or "path" in kwargs:
                self.__create_project_from_file(kwargs.get("path", path), job_id, kwargs.get("name", name), **kwargs)
            elif url or "url" in kwargs:
                self.__create_project_from_url(kwargs.get("url", url), job_id, kwargs.get("name", name), **kwargs)

    def __del__(self):
        if self.id:
            response = self.server.post("command/core/delete-project", {project:self.id})
            if response and response.json["code"] != "ok": print "Request command/core/delete-project failed."# placeholder - do something if it fails?

    def __fetch_new_job(self):
        response = None
        try: response = self.server.post("command/core/create-importing-job")
        except http_exceptions.RequestException: print "Request command/core/create-importing-job failed."
        if response: return response.json["jobID"]

    def __cancel_import_job(self, job_id):
        try: return self.server.post("command/core/cancel-importing-job?jobID={0}".format(job_id))
        except http_exceptions.RequestException: print "Request command/core/cancel-importing-job?jobID={0} failed.".format(job_id)
    @property
    def processes(self):
        try: response = self.server.post("command/core/get-processes?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-processes?project={0} failed.".format(self.id)
        if response: return response.json["processes"]

    @property
    def metadata(self):
        try: return self.server.post("command/core/get-project-metadata?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-project-metadata?project={0}".format(self.id)

    def __get_import_job_status(self, job_id):
        response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
        job = None
        if response and response.json.get("status") == "error": print "Request command/core/get-importing-job-status?jobID={0} returned with error. ".format(job_id) + response.json["job"]["config"]["error"] + response.json["job"]["config"]["errorDetails"] # placeholder - do something because the only response ever is {"status":"error","message":"no such import job"} it means that the job needs to be recreated?
        elif response:
            if response.json["job"]["config"]["state"] == "error": print "Request command/core/get-importing-job-status?jobID={0} returned with error. ".format(job_id) + response.json["job"]["config"]["error"] + response.json["job"]["config"]["errorDetails"] # headers not correct
            job = import_job_details(**response.json["job"]["config"])
            while job.state != "ready":
                sleep(1)
                try:
                    response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
                except Exception:
                    print "Request command/core/get-importing-job-status?jobID={0} failed.".format(job_id)
                    break
            if response: job = import_job_details(response.json["job"]["config"])
        else: raise Exception # odd scenario
        return job

    def __initialize_parser(self, job_id, mime_type="application/json"):
        format = mime_type
        if mime_type not in [f.name for f in self.server.configuration.formats]:
            format = self.server.configuration.mime_types[mime_type]
        if not format: format = "text/json" # set a default
        try: return self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=initialize-parser-ui&format={1}".format(job_id, quote_plus(format)))
        except Exception: print "Failed to initialize-parser-ui."

    def __update_format(self, job_id, refine_mime_type, **kwargs):
        data_options = {
            "text/xml/xlsx":{
                'storeBlankRows': True,
                'ignoreLines': -1,
                'sheetRecords': [],
                'skipDataLines': 0,
                'xmlBased': True,
                'storeBlankCellsAsNulls': True,
                'includeFileSources': False,
                'headerLines': 1,
                'limit': -1
            },
            "text/xml":{
                "recordPath": kwargs.get("recordPath",None),
                "limit":-1,
                "includeFileSources": False,
            },
            "text/json":{
                "recordPath": kwargs.get("recordPath",None),
                "limit":-1,
                "includeFileSources": False,
                "guessCellValueTypes": True
            },
            "text/line-based":{
                'encoding': kwargs.get("encoding", ""),
                'recordPath': kwargs.get("recordPath",None),
                'linesPerRow':1,
                'limit':-1,
                'separator': kwargs.get("separator",None),
                'ignoreLines': -1,
                'headerLines':0,
                'skipDataLines': 0,
                'storeBlankRows': True,
                'guessCellValueTypes': False,
                'processQuotes':False,
                'storeBlankCellsAsNulls': True,
                'includeFileSources': False
            },
            "text/line-based/*sv":{
                'processQuotes': True,
                'storeBlankRows': True,
                'ignoreLines': -1,
                'skipDataLines': 0,
                'separator': kwargs.get("separator",u'\\t'),
                'storeBlankCellsAsNulls': True,
                'guessCellValueTypes': True,
                'includeFileSources': False,
                'headerLines': 1,
            },
            "text/xml/rdf":{
                'includeFileSources': False,
                'encoding': kwargs.get("encoding", "")
            },
            "text/line-based/fixed-width":{
                'storeBlankRows': True,
                'ignoreLines': -1,
                'skipDataLines': 0,
                'storeBlankCellsAsNulls': True,
                'includeFileSources': False,
                'headerLines': 1,
                'encoding' : kwargs.get("encoding", ""),
                'columnWidths': kwargs.get("columnWidths",None),
                'columnNames': kwargs.get("columnNames", None),
                'limit':-1,
                'guessCellValueTypes': False
            },
            "text/line-based/pc-axis":{
                'skipDataLines': 0,
                'limit': -1,
                'includeFileSources': False
            },
            "text/xml/ods":{
                'storeBlankRows': True,
                'ignoreLines': -1,
                'sheetRecords': [],
                'skipDataLines': 0,
                'storeBlankCellsAsNulls': True,
                'includeFileSources': False,
                'headerLines': 1
            }
        }
        self.create_options = data_options[refine_mime_type]
        try:
            headers = {'content-type':'application/x-www-form-urlencoded'}
            return self.server.post("command/core/importing-controller?controller=core/default-importing-controller&jobID={0}&subCommand=update-format-and-options".format(job_id), **{"headers":headers, "data":"format={0}&options={1}".format(quote_plus(refine_mime_type), json.dumps(self.create_options))})
        except Exception: print "Error updating format."

    def __fetch_models(self, job_id):
        try: return self.server.post("command/core/get-models?importingJobID={0}".format(job_id))
        except Exception: print "Unable to retrieve model definitions."

    def __fetch_rows(self, job_id, start=0, max_rows=3):
        try: return self.server.post("command/core/get-rows?importingJobID={0}&start={1}&limit={2}".format(job_id,start,max_rows), **{"data":{"callback":"jsonp{0}".format(randint(1000000000000,1999999999999))}})
        except Exception: print "Unable to retrieve rows."

    def __create(self, job_id, mime_type, name="default", **kwargs):
        try:
            headers = {'content-type':'application/x-www-form-urlencoded'}
            self.create_options["projectName"] = name
            data = "format={0}&options={1}".format(quote_plus(mime_type), quote_plus(json.dumps(self.create_options)))
            return self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=create-project".format(job_id), **{"data":data, "headers":headers})
        except http_exceptions.RequestException: raise

    def __create_project_from_file(self, path, job_id, name, **kwargs):
        files = {'file': (basename(path), open(path, 'rb'))}
        response = self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=load-raw-data".format(job_id), **{"files":files})
        if response and response.json: print "Failed to load data source {0}. ".format(path) + response.json # error message
        job_status = self.__get_import_job_status(job_id) # polls for import completion
        mime_type = job_status.rankedFormats[0]
        format_options = self.__initialize_parser(job_id, mime_type).json
        update_response = self.__update_format(job_id, mime_type, **kwargs)
        model_definitions = self.__fetch_models(job_id).json
        data_preview = self.__fetch_rows(job_id).json
        self.__create(job_id, mime_type, name, **kwargs)

    def __create_project_from_url(self, url, job_id, name, **kwargs):
        mime_type = http_get(url).headers["content-type"]
        if mime_type.find(";") > 0: mime_type = mime_type[0:mime_type.find(";")]
        mime_type = self.server.configuration.mime_types[mime_type]
        boundary = choose_boundary()
        data = "--{0}\r\nContent-Disposition: form-data; name=\"download\"\r\n\r\n{1}\r\n--{0}--".format(boundary,url)
        headers = {"content-type":"multipart/form-data; boundary={0}".format(boundary)}
        response = self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=load-raw-data".format(job_id), **{"data":data, "headers":headers})
        if response and response.json:  print "Failed to load data source {0}. ".format(url) + response.json # error message
        job_status = self.__get_import_job_status(job_id) # polls for import completion
        format_options = self.__initialize_parser(job_id, mime_type).json
        update_response = self.__update_format(job_id, mime_type, **kwargs)
        model_definitions = self.__fetch_models(job_id).json
        data_preview = self.__fetch_rows(job_id).json
        self.__create(job_id, mime_type, name, **kwargs)