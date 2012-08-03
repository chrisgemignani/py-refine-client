from requests import get as http_get, post as http_post, codes as http_codes, exceptions as http_exceptions
from urllib import quote_plus
from random import randint
try: import simplejson as json
except ImportError: import json
from os.path import basename
from time import sleep
from mimetools import choose_boundary
from datetime import datetime




class RefineFormat(object):
    """
    Docstring
    """

    def __init__(self, id=None, name=None, label=None, download=None, ui_class=None, *args, **kwargs):
        self.id = kwargs.get('id', id)
        self.name = kwargs.get('name', name)
        self.label = kwargs.get('label', label)
        self.download = kwargs.get('download', download)
        self.uiClass = kwargs.get('uiClass', ui_class)


class RefineConfiguration(object):
    """
    Docstring
    """

    def __init__(self, **kwargs):
        if kwargs:
            self.formats = (RefineFormat(name=k, **kwargs['formats'][k]) \
                            for k in kwargs['formats'].keys())
            self.mime_types = kwargs['mimeTypeToFormat']
            self.file_extensions = kwargs['extensionToFormat']
        else:
            self.formats = None
            self.mime_types = None
            self.file_extensions = None


class RefineVersion(object):
    """
    Docstring
    """

    def __init__(self, full_name=None, full_version=None, revision=None, version=None, *args, **kwargs):
        self.full_name = kwargs.get('full_name',full_name)
        self.full_version = kwargs.get('full_version', full_version)
        self.revision = kwargs.get('revision', revision)
        self.version = kwargs.get('version', version)


class RefineServer(object):
    """
    Docstring
    """

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
        if response.status_code == http_codes.ok: return RefineVersion(**response.json)

    @property
    def projects(self):
        response = self.get("command/core/get-all-project-metadata")
        if response.status_code == http_codes.ok:
            return (Project(id = pid) for pid in response.json['projects'].keys())
        else: print "Request command/core/get-all-project-metadata failed."

    @property
    def configuration(self):
        response = self.post("command/core/get-importing-configuration")
        if response.status_code == http_codes.ok:
            return RefineConfiguration(**response.json['config'])
        else: print "Request command/core/get-importing-configuration failed."


class DataSource(object):
    """
    Docstring
    """

    def __init__(self, declared_mime_type=None, location=None, file_name=None, origin=None, url=None, size=None, *args, **kwargs):
        self.declared_mime_type=kwargs.get('declaredMimeType', declared_mime_type)
        self.location = kwargs.get('location', location)
        self.fileName = kwargs.get('fileName', file_name)
        self.origin = kwargs.get('origin', origin)
        self.url = kwargs.get('url', url)
        self.size = kwargs.get('size', size)


class RetrievalRecord():
    """
    Docstring
    """

    def __init__(self, files=None, download_count=None, archive_count=None, clipboard_count=None, upload_count=None, *args, **kwargs):
        self.files = (DataSource(**f) for f in kwargs.get('files', files))
        self.download_count = kwargs.get('downloadCount', download_count)
        self.archive_count = kwargs.get('archiveCount', archive_count)
        self.clipboard_count = kwargs.get('clipboardCount', clipboard_count)
        self.upload_count = kwargs.get('uploadCount', upload_count)


class ImportJobDetails():

    def __init__(self, ranked_formats=None, has_data=None, state=None, file_selection=None, retrieval_record=None, *args, **kwargs):
        self.ranked_formats = kwargs.get('rankedFormats', ranked_formats) # array of mime types in order of best guess for this data source
        self.has_data = kwargs.get('hasData', has_data) # boolean
        self.state = kwargs.get('state', state) # "ready"
        self.file_selection = kwargs.get('fileSelection', file_selection) # [0]  ...what does that mean? is it an array of indices that correspond to values in retrievalRecord["files"]?
        if retrieval_record or "retrievalRecord" in kwargs:
            self.retrieval_record = RetrievalRecord(**kwargs.get('retrievalRecord', retrieval_record)) # another object...
        else:
            self.retrieval_record = None


class Project():

    def __init__(self, server=RefineServer(), id=None, path=None, url=None, name=None, *args, **kwargs):
        """
        input kwargs are passed on to __update_formats where they are used to
        determine the first object in an XML/JSON/RDF doc, the encoding, the
        separator for text files (.*sv), and the column widths
        (and optionally headings) for fixed width input

        @param recordPath (required for XML, JSON, line-based and RDF)
            e.g. ["__anonymous__","media","__anonymous__"] where
            __anonymous__ is what refine names a nameless value/object
        @param encoding (optional for RDF and line-based)
            e.g. any valid encoding e.g. "UTF8"
        @separator (optional, defaults to tab, line-based and *sv)
            e.g. "\\t" or "," - any string acceptable
        @columnWidths (required for fixed-width) i.e. a list of character widths
        @columnNames (optional for fixed width) i.e. a list
        """

        self.server = server
        self.id = kwargs.get('id', id)
        self._facets = []
        self._sort_critieria = []
        if not self.id:
            job_id = self._fetch_new_job()
            if path or "path" in kwargs:
                self._create_project_from_file(kwargs.get("path", path), job_id, kwargs.get("name", name), **kwargs)
            elif url or "url" in kwargs:
                self._create_project_from_url(kwargs.get("url", url), job_id, kwargs.get("name", name), **kwargs)

    def __del__(self):
        if self.id:
            response = self.server.post("command/core/delete-project", **{"data":{"project":self.id}})
            if response and response.json["code"] != "ok": print "Request command/core/delete-project failed."# placeholder - do something if it fails?

    def _fetch_new_job(self):
        response = None
        try: response = self.server.post("command/core/create-importing-job")
        except http_exceptions.RequestException: print "Request command/core/create-importing-job failed."
        if response: return response.json["jobID"]

    def _cancel_import_job(self, job_id):
        try: return self.server.post("command/core/cancel-importing-job?jobID={0}".format(job_id))
        except http_exceptions.RequestException: print "Request command/core/cancel-importing-job?jobID={0} failed.".format(job_id)

    @property
    def facets(self):
        return self._facets

    @facets.setter
    def facets(self, new_facet):
        self.facets.append(new_facet)

    def compute_facets(self, mode="row-based"):
        try: return self.server.post("command/core/compute-facets?project={0}".format(self.id), **{"data":{"engine":{"facets":[f.refine_formatted for f in self.facets],"mode":mode}}})
        except http_exceptions.RequestException: print "Request command/core/compute-facets?project={0} failed.".format(self.id)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        try: response = self.server.post("/command/core/rename-column?oldColumnName={0}&newColumnName={1}&project={2}".format(self._name, name, self.id))
        except http_exceptions.RequestException: print "Request /command/core/rename-column?oldColumnName={0}&newColumnName={1}&project={2}".format(self._name, name, self.id)
        if response and response.json.get("code",None) == "ok":
            self._name = name

    @property
    def sort_criteria(self):
        return self._sort_critieria

    @sort_criteria.setter
    def sort_criteria(self, sort_criterion=None):
        # since the sort criteria are stored on the client side, passing in a sort criterion
        # adds to the list of saved criteria and passing in a null value clears the previously
        # saved criteria - should be of type SortCriterion
        if sort_criterion: self._sort_critieria.append(sort_criterion)
        else: self._sort_critieria.clear()

    @property
    def history(self):
        try: response = self.server.post("command/core/get-history?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-history?project={0} failed.".format(self.id)
        if response: return (HistoryEntry(**h) for h in response.json["past"]), (HistoryEntry(**h) for h in response.json["future"])

    @property
    def processes(self):
        try: response = self.server.post("command/core/get-processes?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-processes?project={0} failed.".format(self.id)
        if response: return response.json["processes"]

    @property
    def metadata(self):
        try: return self.server.post("command/core/get-project-metadata?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-project-metadata?project={0}".format(self.id)

    def get_rows(self,offset=0,limit=-1,mode="row-based"):
        try: response = self.server.post("command/core/get-rows?project={0}&start={1}&limit={2}&callback=jsonp{3}".format(self.id,offset,limit,randint(1000000000000,1999999999999)), **{"data":{"engine":{"facets":[f.refine_formatted for f in self.facets],"mode":mode}, "sorting":{"criteria":[s.refine_formatted for s in self.sort_criteria]}}})
        except http_exceptions.RequestException: print "Request command/core/get-rows?project={0}&start={1}&limit={2}&callback=jsonp{3} failed.".format(self.id,offset,limit,randint(1000000000000,1999999999999))
        if response: return response.json

    def transform_column(self, column_name, grel_expression, on_error="keep-original",repeat=False, repeat_count=1):
        # on_error options: keep-original, set-to-blank, store-error
        # repeat default is false but can be set to true in which case repeat_count should be set to the number of iterations
        try: response = self.server.post("command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(column_name, grel_expression, on_error, repeat, repeat_count, self.id))
        except http_exceptions.RequestException: print "Request command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(column_name, grel_expression, on_error, repeat, repeat_count, self.id)

    def _get_import_job_status(self, job_id):
        response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
        job = None
        if response and response.json.get("status") == "error": print "Request command/core/get-importing-job-status?jobID={0} returned with error. ".format(job_id) + response.json["job"]["config"]["error"] + response.json["job"]["config"]["errorDetails"] # placeholder - do something because the only response ever is {"status":"error","message":"no such import job"} it means that the job needs to be recreated?
        elif response:
            if response.json["job"]["config"]["state"] == "error": print "Request command/core/get-importing-job-status?jobID={0} returned with error. ".format(job_id) + response.json["job"]["config"]["error"] + response.json["job"]["config"]["errorDetails"] # headers not correct
            job = ImportJobDetails(**response.json["job"]["config"])
            while job.state != "ready":
                sleep(1)
                try:
                    response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
                except Exception:
                    print "Request command/core/get-importing-job-status?jobID={0} failed.".format(job_id)
                    break
            if response: job = ImportJobDetails(response.json["job"]["config"])
        else: raise Exception # odd scenario
        return job

    def _initialize_parser(self, job_id, mime_type="application/json"):
        format = mime_type
        if mime_type not in [f.name for f in self.server.configuration.formats]:
            format = self.server.configuration.mime_types[mime_type]
        if not format: format = "text/json" # set a default
        try:
            return self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=initialize-parser-ui&format={1}".format(job_id, quote_plus(format)))
        except Exception:
            print "Failed to initialize-parser-ui."

    def _update_format(self, job_id, refine_mime_type, **kwargs):
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

    def _fetch_models(self, job_id):
        try: return self.server.post("command/core/get-models?importingJobID={0}".format(job_id))
        except Exception: print "Unable to retrieve model definitions."

    def _fetch_rows(self, job_id, start=0, max_rows=3):
        try: return self.server.post("command/core/get-rows?importingJobID={0}&start={1}&limit={2}".format(job_id,start,max_rows), **{"data":{"callback":"jsonp{0}".format(randint(1000000000000,1999999999999))}})
        except Exception: print "Unable to retrieve rows."

    def _create(self, job_id, mime_type, name="default", **kwargs):
        try:
            headers = {'content-type':'application/x-www-form-urlencoded'}
            self.create_options["projectName"] = name
            data = "format={0}&options={1}".format(quote_plus(mime_type), quote_plus(json.dumps(self.create_options)))
            return self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=create-project".format(job_id), **{"data":data, "headers":headers})
        except http_exceptions.RequestException: raise

    def _create_project_from_file(self, path, job_id, name, **kwargs):
        files = {'file': (basename(path), open(path, 'rb'))}
        response = self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=load-raw-data".format(job_id), **{"files":files})
        if response and response.json: print "Failed to load data source {0}. ".format(path) + response.json # error message
        job_status = self._get_import_job_status(job_id) # polls for import completion
        mime_type = job_status.rankedFormats[0]
        format_options = self._initialize_parser(job_id, mime_type).json
        update_response = self._update_format(job_id, mime_type, **kwargs)
        model_definitions = self._fetch_models(job_id).json
        data_preview = self._fetch_rows(job_id).json
        self._create(job_id, mime_type, name, **kwargs)

    def _create_project_from_url(self, url, job_id, name, **kwargs):
        mime_type = http_get(url).headers["content-type"]
        if mime_type.find(";") > 0: mime_type = mime_type[0:mime_type.find(";")]
        mime_type = self.server.configuration.mime_types[mime_type]
        boundary = choose_boundary()
        data = "--{0}\r\nContent-Disposition: form-data; name=\"download\"\r\n\r\n{1}\r\n--{0}--".format(boundary,url)
        headers = {"content-type":"multipart/form-data; boundary={0}".format(boundary)}
        response = self.server.post("command/core/importing-controller?controller=core%2Fdefault-importing-controller&jobID={0}&subCommand=load-raw-data".format(job_id),
            **{"data":data, "headers":headers})
        if response and response.json:
            print "Failed to load data source {0}. ".format(url) + response.json
        job_status = self._get_import_job_status(job_id) # polls for import completion
        format_options = self._initialize_parser(job_id, mime_type).json
        update_response = self._update_format(job_id, mime_type, **kwargs)
        model_definitions = self._fetch_models(job_id).json
        data_preview = self._fetch_rows(job_id).json
        self._create(job_id, mime_type, name, **kwargs)

    def split_multi_value_cell(self, column, key_column, separator):
        try:
            return self.server.post("/command/core/split-multi-value-cells?columnName={0}&keyColumnName={1}&separator={2}&mode=plain&project={3}".format(column, key_column, separator, self.id))
        except http_exceptions.RequestException: print "Unable to split cell."

    def compute_facets(self, mode="row-based"):
        try: return self.server.post("command/core/compute-facets?project={0}".format(self.id), **{"data":{"engine":{"mode":mode, "facets":[f.refine_formatted for f in self.facets]}}})
        except Exception: print "Unable to compute facets."

class HistoryEntry(object):

    def __init__(self, *args, **kwargs):
        self.id = kwargs.get("id",None)
        self.description = kwargs.get("description",None)
        self.time = kwargs.get("time",None)
        if self.time:
            self.time = datetime(int(self.time[0:4]), int(self.time[5:7]), int(self.time[8:10]), hour=int(self.time[11:13]), minute=int(self.time[14:16]), second=int(self.time[17:19]))

class SortCriterion(object):

    def __init__(self, column_name, column_type, reverse, blank_position=2, error_position=1, *args, **kwargs):
        # column_type can be string, number, boolean, date
        # string a-z default, z-a is "reverse"
        # number smallest first is default, largest first is "reverse"
        # date earliest first is default, latest first is "reverse"
        # boolean false then true is default, true then false is "reverse"
        if column_type == "string": self.case_sensitive = kwargs.get("case_sensitive",False)
        self.column_name = column_name
        self.column_type = column_type
        self.reverse = reverse
        self.blank_position = blank_position
        self.error_position = error_position

    def refine_formatted(self):
        key_formatted_repr = {}
        for k in self.__dict__.keys():
            new_key = "".join([c.capitalize() for c in k.split("_")])
            key_formatted_repr[new_key[0].lower()+new_key[1:]] = self.__dict__[k]
        return key_formatted_repr


class Facet(object):

    def __init__(self, type, name, column_name, *args, **kwargs):
        self.type = kwargs.get("type", type)
        self.name = kwargs.get("name", name)
        self.column_name = kwargs.get("column_name", column_name)
        for k in kwargs.keys(): self.__dict__[k] = kwargs.get(k, None)

    def refine_formatted(self):
        key_formatted_repr = {}
        for k in self.__dict__.keys():
            if k == "lower_bound": key_formatted_repr["from"] = self.__dict__[k]
            elif k == "upper_bound": key_formatted_repr["to"] = self.__dict__[k]
            else:
                new_key = "".join([c.capitalize() for c in k.split("_")])
                key_formatted_repr[new_key[0].lower()+new_key[1:]] = self.__dict__[k]
        return key_formatted_repr

class ListFacet(Facet):

    def __init__(self, name, column_name, omit_blank, omit_error, selection, select_blank, select_error, invert, expression="value", *args, **kwargs):
        Facet.__init__(self, "list", name, column_name)
        self.expression = expression
        self.omit_blank = omit_blank
        self.omit_error = omit_error
        self.selection = selection # v stands for value and l stands for label: [{"v":{"v":"video","l":"video"}}]
        self.select_blank = select_blank
        self.select_error = select_error
        self.invert = invert

class RangeFacet(Facet):

    def __init__(self, name, column_name, lower_bound, upper_bound, select_numeric=True, select_non_numeric=True, select_blank=True, select_error=True, expression="value", *args, **kwargs):
        Facet.__init__(self, "range", name, column_name)
        self.expression = expression
        self.select_numeric = select_numeric
        self.select_non_numeric = select_non_numeric
        self.select_blank = select_blank
        self.select_error = select_error
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

class TimeRangeFacet(Facet):

    def __init__(self, name, column_name, lower_bound, upper_bound, select_time=True, select_non_time=True, select_blank=True, select_error=True, expression="value", *args, **kwargs):
        Facet.__init__(self, "timerange", name, column_name)
        self.expression = kwargs.get("expression",expression)
        self.select_time = kwargs.get("select_time",select_time)
        self.select_non_time = select_non_time
        self.select_blank = select_blank
        self.select_error = select_error
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

class TextFacet(Facet):

    def __init__(self, name, column_name, mode="text", case_sensitive=False, query=None, *args, **kwargs):
        Facet.__init__(self, "text", name, column_name)
        self.mode = mode
        self.case_sensitive = case_sensitive
        self.query = query