from requests import get as http_get, post as http_post, codes as http_codes, exceptions as http_exceptions
from urllib import quote_plus, quote
from random import randint, choice
from re import match, search
try: import simplejson as json
except ImportError: import json
from os.path import basename
from time import sleep
from mimetools import choose_boundary
from datetime import datetime
from string import letters, digits
from os import remove


DEBUG = False
TIMING = False
DEFAULT_MIME_TYPE = "text/csv"
TMP_DIR = "/tmp/"

if TIMING:
    from time import time

class RefineFormat(object):
    """
    Docstring
    """

    def __init__(self, id=None, name=None, label=None, download=None, ui_class=None, *args, **kwargs):
        self.id = kwargs.get("id", id)
        self.name = kwargs.get("name", name)
        self.label = kwargs.get("label", label)
        self.download = kwargs.get("download", download)
        self.uiClass = kwargs.get("uiClass", ui_class)

    def __unicode__(self):
        return self.name

    def __str__(self):
        return self.__unicode__()


class RefineConfiguration(object):
    """
    Docstring
    """

    def __init__(self, **kwargs):
        if kwargs:
            self.formats = [RefineFormat(name=k, **kwargs["formats"][k]) \
                            for k in kwargs["formats"].keys()]
            self.mime_types = kwargs["mimeTypeToFormat"]
            self.file_extensions = kwargs["extensionToFormat"]
        else:
            self.formats = None
            self.mime_types = None
            self.file_extensions = None

    def __unicode__(self):
        return "Allowed MIME Types: " + ",".join(self.mime_types)

    def __str__(self):
        return self.__unicode__()


class RefineVersion(object):
    """
    Docstring
    """

    def __init__(self, full_name=None, full_version=None, revision=None, version=None, *args, **kwargs):
        self.full_name = kwargs.get("full_name", full_name)
        self.full_version = kwargs.get("full_version", full_version)
        self.revision = kwargs.get("revision", revision)
        self.version = kwargs.get("version", version)

    def __unicode__(self):
        return self.full_version

    def __str__(self):
        return self.__unicode__()


class RefineServer(object):
    """
    Docstring
    """

    def __init__(self, protocol="http", host="localhost", port="3333", *args, **kwargs):
        self.protocol = kwargs.get("protocol", protocol)
        self.host = kwargs.get("host", host)
        self.port = kwargs.get("port", port)

    def __unicode__(self):
        return "{0}://{1}:{2}".format(self.protocol, self.host, self.port)

    def __str__(self):
        return self.__unicode__()

    def get(self, action, stream=False):
        if DEBUG:
            print "REQUEST URL (GET) : {0}/{1}".format(str(self),str(action))
        try:
            response = http_get("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action), stream=stream)
            if response:

                if DEBUG and action.find("get-rows") == -1 and action.find("export-rows") == -1:
                    try:
                        print(("RESPONSE : {0} {1}").format(response.status_code, unicode(response.text)))
                    except Exception as e:
                        print "DEBUG ERROR : {0}".format(e)

                if response.status_code == 500:
                    raise Exception("{0} returned with 500.".format(action))
                else:
                    return response

            else:
                print "No response returned from {0}".format(action)

        except http_exceptions.RequestException as e:
            print "Request {0} failed. {1}".format(action, e)


    def post(self, action, data=None, headers=None, files=None, stream=False, **kwargs):
        if DEBUG:
            print "REQUEST URL (POST) : {3}/{0}\nDATA : {1}\nHEADERS : {2}".format(str(action),
                                                                                   str(kwargs.get("data", str(data))),
                                                                                   str(kwargs.get("headers", str(headers))),
                                                                                   str(self))
        try:
            new_kwargs = {"data": kwargs.get("data", data),
                          "files": kwargs.get("files", files),
                          "headers": kwargs.get("headers", headers),
                          "stream": kwargs.get("stream", stream)}
            new_kwargs.update(kwargs)
            if new_kwargs["data"]:
                for k in new_kwargs["data"].keys():
                    if isinstance(new_kwargs["data"][k], dict):
                        new_kwargs["data"][k] = json.dumps(new_kwargs["data"][k])
                if DEBUG:
                    print "MODIFIED DATA {0}".format(new_kwargs["data"])
            response = http_post("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action),
                                 **new_kwargs)
            if response:
                if DEBUG and action.find("get-rows") == -1 and action.find("export-rows") == -1:
                    try:
                        print(("RESPONSE : {0} {1}").format(response.status_code, unicode(response.text)))
                    except Exception as e:
                        print "DEBUG ERROR : {0}".format(e)

                if response.status_code == 500:
                    raise Exception("{0} returned with 500.".format(action))
                else:
                    return response

            else:
                print "No response returned from {0}".format(action)

        except http_exceptions.RequestException as e:
            print "Request {0} failed. {1}".format(action, e)


    def destroy_all_projects(self):
        for p in self.projects:
            if DEBUG:
                print "Destroying {0}".format(p.id)
            p.destroy()

    @property
    def version(self):
        response = self.get("command/core/get-version")
        if response.status_code == http_codes.ok:
            try:
                return RefineVersion(**response.json())
            except Exception as e:
                raise Exception("Failed to retrieve JSON from get-version request. {0}".format(e.message))

    @property
    def projects(self):
        response = self.get("command/core/get-all-project-metadata")
        if response.status_code == http_codes.ok:
            try:
                json_response = response.json()
            except Exception as e:
                raise Exception("Failed to retrieve projects. {0}".format(e.message))
            return [Project(id=pid) for pid in json_response.get("projects",{}).keys()]
        else:
            print "Request command/core/get-all-project-metadata failed."

    @property
    def configuration(self):
        response = self.post("command/core/get-importing-configuration")
        if response.status_code == http_codes.ok:
            try:
                json_response = response.json()
                return RefineConfiguration(**json_response.get("config",{}))
            except Exception as e:
                raise Exception("Failed to retrieve JSON for get-importing-configuration request. {0}".format(e.message))
        else:
            print "Request command/core/get-importing-configuration failed."

    @staticmethod
    def simple_quote(unsafe):
        quoted = quote_plus(unsafe.encode('utf-8'))
        return quoted

class DataSource(object):
    """
    Docstring
    """

    def __init__(self, declared_mime_type=None, location=None, file_name=None, origin=None, url=None,
                 size=None, *args, **kwargs):
        self.declared_mime_type = kwargs.get("declaredMimeType", declared_mime_type)
        self.location = kwargs.get("location", location)
        self.fileName = kwargs.get("fileName", file_name)
        self.origin = kwargs.get("origin", origin)
        self.url = kwargs.get("url", url)
        self.size = kwargs.get("size", size)

    def __unicode__(self):
        return "{0} {1} {2} {3}".format(self.location, self.fileName, self.declared_mime_type, self.size)

    def __str__(self):
        return self.__unicode__()


class RetrievalRecord():
    """
    Docstring
    """

    def __init__(self, files=None, download_count=None, archive_count=None, clipboard_count=None,
                 upload_count=None, *args, **kwargs):
        self.files = [DataSource(**f) for f in kwargs.get("files", files)]
        self.download_count = kwargs.get("downloadCount", download_count)
        self.archive_count = kwargs.get("archiveCount", archive_count)
        self.clipboard_count = kwargs.get("clipboardCount", clipboard_count)
        self.upload_count = kwargs.get("uploadCount", upload_count)

    def __unicode__(self):
        return ("{0} downloads, {1} archives, "
                "{2} clipboards, {3} uploads\n{4}").format(self.download_count, self.archive_count,
                                                           self.clipboard_count, self.upload_count, str(self.files))

    def __str__(self):
        return self.__unicode__()


class ImportJobDetails():
    def __init__(self, ranked_formats=None, has_data=None, state=None, file_selection=None,
                 retrieval_record=None, progress=None, *args, **kwargs):
        self.ranked_formats = kwargs.get("rankedFormats",
                                         ranked_formats) # array of mime types in order of best guess for this data source
        self.has_data = kwargs.get("hasData", has_data)
        self.state = kwargs.get("state", state)
        self.progress = kwargs.get("progress", progress)
        self.file_selection = kwargs.get("fileSelection",
                                         file_selection) # an array of indices that correspond to values in retrievalRecord["files"]
        if retrieval_record or "retrievalRecord" in kwargs:
            self.retrieval_record = RetrievalRecord(**kwargs.get("retrievalRecord", retrieval_record))
        else:
            self.retrieval_record = None

    def __unicode__(self):
        return ("Job is {0} and {1}"
                "\nFormats (most likely first):\n{2}").format(self.state, "has data" if self.has_data else "has no data",
                                                              str(self.ranked_formats))

    def __str__(self):
        return self.__unicode__()


class ColumnDefinition():
    def __init__(self, name=None, original_name=None, index=None, *args, **kwargs):
        self.cell_index = kwargs.get("cellIndex", index)
        self.original_name = kwargs.get("originalName", original_name)
        self.name = kwargs.get("name", name)

    def __unicode__(self):
        return "[{0}] {1} (originally named {2})".format(self.cell_index, self.name, self.original_name)

    def __str__(self):
        return self.__unicode__()


class RowSet():
    def __init__(self, offset, limit, filtered_count, total_count, json_rows, *args, **kwargs):
        self.offset = kwargs.get("offset", offset)
        self.limit = kwargs.get("limit", limit)
        self.filtered_count = kwargs.get("filtered", filtered_count)
        self.total_count = kwargs.get("total", total_count)
        self.rows = kwargs.get("rows", json_rows)

    def __unicode__(self):
        return ("{0} rows starting at {1} of {2} "
                "total rows ({3} filtered rows)\nSample Row:\n{4}".format(self.limit, self.offset, self.total_count,
                                                                          self.filtered_count,
                                                                          str(self.rows[0]) if len(self.rows) > 0
                                                                          else "["
                                                                               "no"
                                                                               " "
                                                                               "rows]"))

    def __str__(self):
        return self.__unicode__()


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
        self.id = kwargs.get("id", id)
        self._facets = []
        self._sort_criteria = []
        self._columns = []
        self.row_set = None
        if not self.id:
            if TIMING: start = time()
            job_id = self._fetch_new_job()
            if TIMING: print "REFINE : fetching new job ID took {0}".format(time() - start)
            if path or "path" in kwargs:
                p = kwargs.get("path",path)
                try:
                    open(p, "r")
                    self._create_project_from_file(kwargs.get("path", path), job_id, kwargs.get("name", name), **kwargs)
                except Exception as e:
                    print "Failed to retrieve local path {0}. Attempting to load from url.".format(p)
                    if url or "url" in kwargs:
                        self._create_project_from_url(kwargs.get("url", url), job_id, kwargs.get("name", name), **kwargs)
                    else:
                        raise Exception("Unable to retrieve {0} and no url available for retrieval.".format(p))

            elif url or "url" in kwargs:
                self._create_project_from_url(kwargs.get("url", url), job_id, kwargs.get("name", name), **kwargs)

    def __unicode__(self):
        return "Project ID {0} using server at {1}".format(self.id, unicode(self.server))

    def __str__(self):
        return self.__unicode__()

    def destroy(self):
        if self.id:
            self.server.post("command/core/delete-project", **{"data": {"project":self.id}, "timeout":0.1})

    def _fetch_new_job(self):
        response = None
        try: response = self.server.post("command/core/create-importing-job")
        except http_exceptions.RequestException: print "Request command/core/create-importing-job failed."
        if response:
            try:
                json_response = response.json()
            except Exception as e:
                raise Exception("Failed to retrieve JSON response to create-importing-job request. {0}".format(e.message))
            return json_response.get("jobID", None)

    def _cancel_import_job(self, job_id):
        try: return self.server.post("command/core/cancel-importing-job?jobID={0}".format(job_id))
        except http_exceptions.RequestException: print("Request command/core/cancel-importing-job?"
                                                       "jobID={0} failed.".format(job_id))

    @property
    def facets(self):
        return self._facets

    def append_facet(self, new_facet):
        self._facets.append(new_facet)

    def clear_facets(self):
        self._facets = []

    @property
    def name(self):
        return self._name

    def rename_column(self, old_name, new_name):
        try:
            response = self.server.post("command/core/rename-column?oldColumnName={0}"
                                        "&newColumnName={1}&project={2}".format(RefineServer.simple_quote(old_name),
                                                                                RefineServer.simple_quote(new_name),
                                                                                self.id))
        except http_exceptions.RequestException:
            print("Request /command/core/rename-column?oldColumnName={0}"
                  "&newColumnName={1}&project={2}".format(RefineServer.simple_quote(old_name),
                                                          RefineServer.simple_quote(new_name),
                                                          self.id))

    @property
    def sort_criteria(self):
        return self._sort_criteria

    def append_sort(self, new_sort):
        self._sort_criteria.append(new_sort)

    def clear_sort(self):
        self._sort_criteria = []

    @property
    def history(self):
        try: response = self.server.get("command/core/get-history?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-history?project={0} failed.".format(
            self.id)
        if response:
            try:
                json_response = response.json()
                return [HistoryEntry(**h) for h in json_response.get("past", [])], [HistoryEntry(**h) for h in
                                                                                    json_response.get("future", [])]
            except Exception as e:
                raise Exception("Failed to load JSON in response to get-history request. {0}".format(e.message))


    @property
    def processes(self):
        try: response = self.server.get("command/core/get-processes?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-processes?project={0} failed.".format(
            self.id)
        if response:
            try:
                json_response = response.json()
                return json_response.get("processes")
            except Exception as e:
                raise Exception("Failed to load JSON in response to get-processes request. {0}".format(e.message))


    @property
    def metadata(self):
        try: return self.server.get("command/core/get-project-metadata?project={0}".format(self.id))
        except http_exceptions.RequestException: print "Request command/core/get-project-metadata?project={0}".format(
            self.id)

    @property
    def column_names(self):
        return map(lambda c: c.name, self._columns)

    @property
    def columns(self):
        self._fetch_models()
        return self._columns

    def rows(self, job_id=None, offset=0, limit=500000, mode="row-based"):
        if TIMING: start = time()
        try:
            if job_id:
                data = {"callback": "jsonp{0}".format(randint(1000000000000, 1999999999999))}
                response = self.server.post(
                    "command/core/get-rows?importingJobID={0}&start={1}&limit={2}".format(job_id, offset,
                                                                                          quote(str(limit))),
                    **{"data": data, "stream": True})
            else:
                callback = "jsonp{0}".format(randint(1000000000000, 1999999999999))
                data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode},
                        "sorting": {"criteria": [s.refine_formatted_keys() for s in self
                        .sort_criteria]},
                        "callback": callback}
                if DEBUG:
                    print "DATA {0}".format(data)
                response = self.server.post(("command/core/get-rows?project={0}&start={1}&limit={2}"
                                             "&callback={3}".format(self.id, offset, quote(str(limit)), callback)),
                                            **{"data": data, "stream": True})
        except Exception as e:
            print "Unable to retrieve rows. {0}".format(e)

        if TIMING: print "REFINE : getting rows took {0} seconds".format(time() - start)

        if response:
            response = json.loads(response.text[19:-1])
            self.row_set = RowSet(offset, limit, response["filtered"], response["total"], response["rows"])
            return self.row_set

        else: return None

    def export(self, filename, template, format="template", prefix="{\"rows\":[", suffix="]}",
               separator=",", sorting=[], facets=[], mode="row-based", stream=True, *args, **kwargs):

        if TIMING: start = time()
        data = {"project": self.id,
                "engine": {"facets": [f.refine_formatted_keys() for f in kwargs.get("facets", facets)],
                           "mode": kwargs.get("mode", mode)},
                "template": kwargs.get("template", template),
                "format": kwargs.get("format", format),
                "prefix": kwargs.get("prefix", prefix),
                "suffix": kwargs.get("suffix", suffix),
                "sorting": {"criteria": [s.refine_formatted_keys() for s in kwargs.get("sorting", sorting)]},
                "separator": kwargs.get("separator", separator)}
        try:
            response = self.server.post("command/core/export-rows/{0}".format(filename),
                                        **{"data": data, "stream": stream})
        except http_exceptions.RequestException: print "Failed to export rows."
        local_path = '{0}{1}.json'.format(TMP_DIR, "{0}".format("".join(choice(letters+digits) for _ in xrange(20))))
        with open(local_path, 'wb') as rows_file:
            rows_file.write(response.content)
        if TIMING: print "REFINE : exporting took {0} seconds".format(time() - start)
        return local_path

    @staticmethod
    def prepare_qs_expression(base_expression):
        clean_expression = RefineServer.simple_quote(base_expression)
        if not match("^(?:grel)|(?:jython)|(?:clojure):", clean_expression):
            clean_expression = "grel:" + clean_expression
        if DEBUG:
            print clean_expression
        return clean_expression

    def transform_column(self, column_name, expression, on_error="keep-original", repeat=False, repeat_count=1, *args, **kwargs):
        if TIMING: start = time()
        """
        on_error options: keep-original, set-to-blank, store-error
        repeat default is false but can be set to true in which case repeat_count should be set to the number of iterations
        """
        try:
            response = self.server.post(
                "command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(
                    RefineServer.simple_quote(column_name),
                    Project.prepare_qs_expression(expression),
                    kwargs.get("on_error", on_error),
                    kwargs.get("repeat", repeat),
                    kwargs.get("repeat_count", repeat_count),
                    self.id))
        except http_exceptions.RequestException:
            print "Request command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(
                RefineServer.simple_quote(column_name),
                Project.prepare_qs_expression(expression),
                kwargs.get("on_error", on_error),
                kwargs.get("repeat", repeat),
                kwargs.get("repeat_count", repeat_count),
                self.id)
        if TIMING: print "REFINE : transforming column took {0} seconds".format(time() - start)

    def _get_import_job_status(self, job_id):

        response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
        job_status = None
        if response:
            try:
                json_response = response.json()
            except Exception as e:
                if response.text:
                    raise Exception("Failed to retrieve JSON from get-importing-job-status. {0} : {1}".format(e.message,
                                                                                                              response.text))
                else:
                    raise Exception("Failed to retrieve JSON from get-importing-job-status. {0} : Failed to load response text."
                    .format(e
                    .message))

            if json_response.get("status") == "error":
                try:
                    raise Exception("Request command/core/get-importing-job-status?"
                                    "jobID={0} returned with error.\n{1}\n{2}".format(job_id,
                                                                                      json_response["job"]["config"].get("error",
                                                                                                                         "[no message]"),
                                                                                      json_response["job"]["config"].get("errorDetails",
                                                                                                                         "[no details]")))
                except Exception as e:
                    raise Exception("Unable to print result of get-importing-job-status but it has failed [1] :".format(e))

            else:

                if json_response["job"]["config"].get("state", None) == "error":
                    try:
                        raise Exception("Request command/core/get-importing-job-status?"
                                        "jobID={0} returned with error.\n{1}\n{2}".format(job_id,
                                                                                          json_response["job"]["config"].get("error",
                                                                                                                             "[no message]"),
                                                                                          json_response["job"]["config"].get("errorDetails",
                                                                                                                             "[no details]")))
                    except Exception as e:
                        raise Exception("Unable to print result of get-importing-job-status but it has failed [2] : {0}".format(e))

                job_status = ImportJobDetails(**json_response.get("job").get("config"))
                while not (job_status.state == "ready" or job_status.state == "created-project"):

                    sleep(0.25)

                    try:
                        response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
                        try:
                            json_response = response.json()
                        except Exception as e:
                            if response.text:
                                raise Exception("Failed to retrieve JSON from get-importing-job-status. {0} : {1}".format(e.message,
                                                                                                                          response.text))
                            else:
                                raise Exception("Failed to retrieve JSON from get-importing-job-status. {0} : Failed to load response text."
                                .format(e.message))
                        job_status = ImportJobDetails(**json_response.get("job").get("config"))

                    except Exception as e:
                        print "Request command/core/get-importing-job-status?jobID={0} failed with {1}.".format(job_id,
                                                                                                                e.message)
                        break
                if job_status.state == "created-project":
                    self.id = json_response["job"]["config"]["projectID"]

        else:
            print "No response to get-importing-job-status"
            raise Exception # odd scenario

        return job_status


    def _initialize_parser(self, job_id, mime_type="application/json"):

        format = mime_type
        if DEBUG:
            print "Initialize parser received : {0}".format(mime_type)

        if mime_type not in [f.name for f in self.server.configuration.formats]:
            format = self.server.configuration.mime_types[mime_type]

        if not format: format = DEFAULT_MIME_TYPE # set a default

        if DEBUG:
            print "Initializing parser to {0}".format(format)

        response = ""
        try:
            response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                         "&jobID={0}&subCommand=initialize-parser-ui&format={1}".format(job_id,
                                                                                                        RefineServer.simple_quote(
                                                                                                            format))))
            # if file extension is xls but the file is actually xlsx an HTML error page is returned and, unseen,
            # hangs the process in an infinite loop
            try:
                json_response = response.json()
                return (mime_type, json_response.get("options"))
            except Exception as e:
                raise Exception("Failed to retrieve JSON in response to initialize-parser-ui request. {0}".format(e.message))

        except Exception as e:
            print "Failed to initialize-parser-ui. {0}".format(e.message)
            if format == "binary/xls":
                format = "text/xml/xlsx"
                try:
                    response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                                 "&jobID={0}&subCommand=initialize-parser-ui&format={1}".format(job_id,
                                                                                                                RefineServer.simple_quote(
                                                                                                                    format))))
                    try:
                        json_response = response.json()
                        return (format, json_response.get("options"))
                    except Exception as e:
                        raise Exception("Failed to retrieve JSON in response to initialize-parser-ui request. {0}".format(e
                        .message))

                except Exception as e:
                    print "Failed to initialize-parser-ui with fallback format. {0}".format(e.message)
                    # the process needs to stop here because it is likely trying to push an HTML prompt
                    raise Exception("Failed to initialize-parser-ui with fallback format. {0}".format(e.message))


    def _update_format(self, job_id, refine_mime_type, **kwargs):

        data_options = {
            "binary/xls": {
                "xmlBased": kwargs.get("xml_based", kwargs.get("xmlBased", False)),
                "sheets": kwargs.get("sheets", [0]),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1)),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "limit": kwargs.get("limit", -1),
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False))
            },
            "text/xml/xlsx": {
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "sheets": kwargs.get("sheets", [0]),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "xmlBased": kwargs.get("xml_based", kwargs.get("xmlBased", True)),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1)),
                "limit": kwargs.get("limit", -1)
            },
            "text/xml": {
                "recordPath": kwargs.get("recordPath", kwargs.get("record_path", None)),
                "limit": kwargs.get("limit", -1),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                },
            "text/json": {
                "recordPath": kwargs.get("recordPath", kwargs.get("record_path", None)),
                "limit": kwargs.get("limit", -1),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "guessCellValueTypes": kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", True))
            },
            "text/line-based": {
                "encoding": kwargs.get("encoding", ""),
                "recordPath": kwargs.get("recordPath", kwargs.get("record_path", None)),
                "linesPerRow": kwargs.get("lines_per_row", kwargs.get("linesPerRow", 1)),
                "limit": kwargs.get("limit", -1),
                "separator": kwargs.get("separator", None),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1)),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "guessCellValueTypes": kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", False)),
                "processQuotes": kwargs.get("process_quotes", kwargs.get("processQuotes", False)),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False))
            },
            "text/line-based/*sv": {
                "processQuotes": kwargs.get("process_quotes", kwargs.get("processQuotes", True)),
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "separator": kwargs.get("separator", u"\\t"),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "guessCellValueTypes": kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1)),
                },
            "text/xml/rdf": {
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "encoding": kwargs.get("encoding", "")
            },
            "text/line-based/fixed-width": {
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1)),
                "encoding": kwargs.get("encoding", ""),
                "columnWidths": kwargs.get("column_widths", kwargs.get("columnWidths", None)),
                "columnNames": kwargs.get("column_names", kwargs.get("columnNames", None)),
                "limit": kwargs.get("limit", -1),
                "guessCellValueTypes": kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", False))
            },
            "text/line-based/pc-axis": {
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "limit": kwargs.get("limit", -1),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False))
            },
            "text/xml/ods": {
                "sheets": kwargs.get("sheets", [0]),
                "limit": kwargs.get("limit", -1),
                "storeBlankRows": kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", False)),
                "ignoreLines": kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
                "sheetRecords": kwargs.get("sheet_records", kwargs.get("sheetRecords", [])),
                "skipDataLines": kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
                "storeBlankCellsAsNulls": kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
                "includeFileSources": kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
                "headerLines": kwargs.get("header_lines", kwargs.get("headerLines", 1))
            }
        }
        self.create_options = data_options[refine_mime_type]

        try:
            headers = {"content-type": "application/x-www-form-urlencoded"}
            data = {"format": refine_mime_type,
                    "options": self.create_options}
            return self.server.post(("command/core/importing-controller?controller=core/default-importing-controller"
                                     "&jobID={0}&subCommand=update-format-and-options".format(job_id)),
                                    **{"data": data, "headers": headers})
        except Exception: print "Error updating format."


    def _fetch_models(self, job_id=None):

        try:
            if job_id:
                response = self.server.post("command/core/get-models?importingJobID={0}".format(job_id))
            else:
                response = self.server.post("command/core/get-models?project={0}".format(self.id))

            try:
                json_response = response.json()
            except Exception as e:
                raise Exception("Failed to load JSON in response to get-models request. {0}".format(e.message))

        except Exception as e:
            print "Unable to retrieve model definitions. {0}".format(e.message)

        if json_response.get("columnModel", None) and json_response.get("columnModel").get("columns", None):
            self._columns = [ColumnDefinition(**c) for c in json_response["columnModel"].get("columns", [])]
            self._columns.sort(key=lambda i: i.cell_index)
        if DEBUG: print self._columns


    def _create(self, job_id, mime_type, name="default", **kwargs):

        try:
            headers = {"content-type": "application/x-www-form-urlencoded"}
            self.create_options["projectName"] = name
            data = {"format": mime_type,
                    "options": self.create_options}
            response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                         "&jobID={0}&subCommand=create-project".format(job_id)),
                                        **{"data": data, "headers": headers})
            # _get_import_job_status returns more information with each successive request involved in the project creation process
            # after create-project it shows the projectID and is the only way of discovering this value given a job id
            job_status = self._get_import_job_status(job_id)
            return response
        except http_exceptions.RequestException: raise


    def _create_project_from_file(self, path, job_id, name, **kwargs):

        files = {"file": (basename(path), open(path, "rb"))}

        if TIMING: start = time()
        response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                     "&jobID={0}&subCommand=load-raw-data".format(job_id)), **{"files": files})
        if response:
            # should not return a response - occasionally fails for no reason citing non-existent job id - force retry
            sleep(1)
            response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                         "&jobID={0}&subCommand=load-raw-data".format(job_id)), **{"files": files})
            try:
                json_response = response.json()
                if json_response:
                    try:
                        print "Failed to load data source {0}.\n{1}".format(path, response.json()) # error message
                    except Exception as e:
                        print "Failed to load data source {0}! Failed to display error message in _create_project_from_file! {1}" \
                            .format(path, e)
            except Exception as e:
                print "Failed to load JSON in response to load-raw-data request - but there should be no JSON if this request" \
                      " is successful so we are probably okay. {0}".format(e.message)

        if TIMING: print "REFINE : load raw data took {0} seconds".format(time() - start)

        if TIMING: start = time()
        job_status = self._get_import_job_status(job_id) # polls for import completion
        if TIMING: print "REFINE : polling for status took {0} seconds".format(time() - start)

        if DEBUG:
            print job_status

        mime_type = job_status.ranked_formats[0]
        if mime_type == "text/line-based" or mime_type == "text/line-based/fixed-width" and kwargs.has_key("separator"):
            if kwargs.get("separator") == ",":
                mime_type = "text/line-based/*sv"

        if mime_type=="text/json" and not kwargs.has_key("record_path") and not kwargs.has_key("recordPath"):
            if TIMING: start = time()
            kwargs["recordPath"] = Project.identify_json_record_path(None, path)
            if TIMING: print "REFINE : identifying JSON path took {0} seconds".format(time() - start)

        elif mime_type=="text/line-based/*sv" and not kwargs.has_key("separator"):
            if TIMING: start = time()
            kwargs["separator"]=Project.sv_separator(None, path)
            if TIMING: print "REFINE : identifying *SV separator took {0} seconds".format(time() - start)
            if DEBUG:
                print "Selected *sv separator {0}".format(str(kwargs["separator"]))

        if TIMING: start = time()
        (mime_type, presets) = self._initialize_parser(job_id, mime_type)
        if TIMING: print "REFINE : initializing parser took {0} seconds".format(time() - start)
        presets.update(kwargs)
        if DEBUG:
            print "Presets : {0}".format(str(presets))
        if TIMING: start = time()
        update_response = self._update_format(job_id, mime_type, **presets)
        if TIMING: print "REFINE : updating format took {0} seconds".format(time() - start)

        if TIMING: start = time()
        self._fetch_models(job_id)
        if TIMING: print "REFINE : fetching models took {0} seconds".format(time() - start)

        if TIMING: start = time()
        self._create(job_id, mime_type, name, **kwargs)
        if TIMING: print "REFINE : creating project took {0} seconds".format(time() - start)


    def _create_project_from_url(self, url, job_id, name, **kwargs):

        if DEBUG:
            print "Fetching {0}".format(url)

        mime_type = http_get(url).headers["content-type"]

        if mime_type.find(";") > 0:
            mime_type = mime_type[0:mime_type.find(";")]
        if DEBUG:
            print "Provided MIME Type : {0}".format(mime_type)

        tmp_mime_type = self.server.configuration.mime_types.get(mime_type, None)
        if not tmp_mime_type:
            print "Could not configure mime type!"
            if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                print "All is well - Open Office determined - setting appropriately."
                mime_type = "text/xml/xlsx"
            else:
                raise AttributeError("Uh oh... Problem matching {0} in available {1}".format(mime_type,
                                                                                             str(self.server.configuration.mime_types)))
        else:
            mime_type = tmp_mime_type

        if mime_type=="text/json" and not kwargs.has_key("record_path") and not kwargs.has_key(
            "recordPath"):
            if TIMING: start = time()
            if DEBUG: print "JSON. Tracking record path..."
            kwargs["recordPath"] = Project.identify_json_record_path(url)
            if TIMING: print "REFINE : identifying JSON path took {0} seconds".format(time() - start)
        elif mime_type=="text/line-based/*sv" and not kwargs.has_key("separator"):
            if TIMING: start = time()
            if DEBUG: print "Text. Locating separator..."
            kwargs["separator"]=Project.sv_separator(url)
            if TIMING: print "REFINE : identifying *SV separator took {0} seconds".format(time() - start)
            if DEBUG:
                print "Selected *sv separator {0}".format(str(kwargs["separator"]))

        boundary = choose_boundary()
        if TIMING: start = time()
        data = "--{0}\r\nContent-Disposition: form-data; name=\"download\"\r\n\r\n{1}\r\n--{0}--".format(boundary, url)
        headers = {"content-type": "multipart/form-data; boundary={0}".format(boundary)}
        response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                     "&jobID={0}&subCommand=load-raw-data".format(job_id)),
                                    **{"data": data, "headers": headers})
        # should have no response - quick indicator of failure - occasionally happens without cause - force retry
        if response:
            sleep(1)
            response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
                                         "&jobID={0}&subCommand=load-raw-data".format(job_id)),
                                        **{"data": data, "headers": headers})
            if response:
                try:
                    json_response = response.json()
                    if json_response:
                        try:
                            print "Failed to load data source {0}.\n{1}".format(path, response.text[0:300]) # error message
                        except Exception as e:
                            print "Failed to display error message in _create_project_from_file! {0}".format(e.message)
                except Exception as e:
                    print "Failed to load JSON in response to load-raw-data request - but there should be no JSON if this request" \
                          " is successful so we are probably okay. {0}".format(e.message)

        if TIMING: print "REFINE : load raw data took {0} seconds".format(time() - start)

        if TIMING: start = time()
        job_status = self._get_import_job_status(job_id) # polls for import completion
        if TIMING: print "REFINE : polling for status took {0} seconds".format(time() - start)

        if mime_type == "text/line-based" and job_status.ranked_formats[0] != mime_type:
            mime_type = job_status.ranked_formats[0]

        if DEBUG:
            print "Running with MIME Type : {0}".format(mime_type)
        if mime_type:
            if TIMING: start = time()
            (mime_type, presets) = self._initialize_parser(job_id, mime_type)
            if TIMING: print "REFINE : initializing parser took {0} seconds".format(time() - start)
        presets.update(kwargs)
        if DEBUG:
            print "Presets {0}".format(str(presets))
        if TIMING: start = time()
        update_response = self._update_format(job_id, mime_type, **presets)
        if TIMING: print "REFINE : updating format took {0} seconds".format(time() - start)

        if TIMING: start = time()
        self._fetch_models(job_id)
        if TIMING: print "REFINE : fetching models took {0} seconds".format(time() - start)

        if TIMING: start = time()
        self._create(job_id, mime_type, name, **presets)
        if TIMING: print "REFINE : creating project took {0} seconds".format(time() - start)

    @staticmethod
    def sv_separator(url=None, path=None):

        def cleanup(local_path):
            try:
                remove(local_path)
            except Exception as e:
                print "Could not delete local copy of file {0} : {1}".format(local_path, e)

        content_line = ""
        if url:
            response = http_get(url, stream=True)
            local_path = '{0}{1}'.format(TMP_DIR, "".join(choice(letters+digits) for _ in xrange(20)))
            with open(local_path, 'wb') as original_file:
                original_file.write(response.content)
            with open(local_path,"r") as f:
                content_line = f.readline()
        elif path:
            with open(path,"r") as f:
                content_line = f.readline()

        if match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|,)+$", content_line):
            try:
                cleanup(local_path)
            except Exception as e: print "Could not destroy tmp file! {0}".format(e)
            return ","
        elif match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|\t)+$", content_line):
            try:
                cleanup(local_path)
            except Exception as e: print "Could not destroy tmp file! {0}".format(e)
            return u"\\t"
        elif match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|;)+$", content_line):
            try:
                cleanup(local_path)
            except Exception as e: print "Could not destroy tmp file! {0}".format(e)
            return ";"
        else:
            try:
                cleanup(local_path)
            except Exception as e: print "Could not destroy tmp file! {0}".format(e)
            return "," # and hope for the best


    @staticmethod
    def identify_json_record_path(url=None, path=None):

        if url:
            content = http_get(url, stream=True)
            try:
                content = content.json()
            except Exception as e:
                raise Exception("Failed to load raw data in order to identify JSON record path! {0}".format(e.message))

        elif path:
            content = json.loads(file.open(path,"r").read())
        path = []

        # Note that google refine expects a nameless node to be specified as "__anonymous__", e.g. the root node
        def recurse(node, route):
            if DEBUG:
                print "Route state {0}".format(route)
            if isinstance(node, dict):
                if DEBUG:
                    print "Node is a dictionary."
                if len(route) == 0:
                    route.append("__anonymous__")
                for k in node.keys():
                    if DEBUG:
                        print "Recursing on key {0}.".format(k)
                    r = list(route)
                    r.append(k)
                    new_route = recurse(node[k], r)
                    if new_route: return new_route
            elif isinstance(node, list):
                if DEBUG:
                    print "Node is a list."
                if len(route) == 0:
                    return route # because the root node is a list - treat it as the recordset
                else:
                    # detect if all elements are dicts with similar sets of keys
                    if len(node) > 1:
                        if node[0].keys() == node[1].keys() or (set(node[0].keys()) <= set(node[1].keys())) or (
                                set(node[1].keys()) <= set(node[0].keys())):
                            if DEBUG:
                                print "List passed key test! Found recordset!"
                            return route
                        else:
                            if DEBUG:
                                print "Key sets do not match!"
                    elif len(node) == 1:
                        return route


        path = recurse(content, path)
        path.append("__anonymous__")
        if DEBUG:
            print "RECORD PATH : {0}".format(path)
            # refine wants the first record node, not the list node comprising it
        return path

    def split_multi_value_cell(self, column_name, key_column, separator):
        try:
            response = self.server.post(("command/core/split-multi-value-cells?columnName={0}&keyColumnName={1}"
                                         "&separator={2}&mode=plain&project={3}".format(RefineServer.simple_quote(column_name),
                                                                                        RefineServer.simple_quote(key_column),
                                                                                        separator, self.id)))
        except http_exceptions.RequestException: print "Unable to split cell."

    def split_column_by_separator(self, column_name, separator=",", regex=False, remove_original=True,
                                  guess_cell_type=True):
        try:
            response = self.server.post(("command/core/split-column?columnName={0}&mode=separator&project={1}"
                                         "&guessCellType={2}&removeOriginalColumn={3}&separator={4}"
                                         "&regex={5}".format(RefineServer.simple_quote(column_name), self.id,
                                                             str(guess_cell_type).lower(),
                                                             str(remove_original).lower(),
                                                             RefineServer.simple_quote(separator),
                                                             RefineServer.simple_quote(regex) if regex else "false")))
            self._fetch_models()
        except http_exceptions.RequestException: print "Unable to split column."

    def split_column_by_field_length(self, column_name, lengths, remove_original=True, guess_cell_type=True):
        try:
            response = self.server.post(("command/core/split-column?columnName={0}&mode=lengths&project={1}"
                                         "&guessCellType={2}&removeOriginalColumn={3}"
                                         "&fieldLengths={4}".format(RefineServer.simple_quote(column_name), self.id,
                                                                    str(guess_cell_type).lower(),
                                                                    str(remove_original).lower(),
                                                                    RefineServer.simple_quote(lengths))))
            self._fetch_models()
        except http_exceptions.RequestException: print "Unable to split column."
        return response

    def edit_cell(self, row_index, column_index, new_value, type="text", facets=[], mode="row-based"):
        """
        type can be text, number, boolean, date
        """
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in facets],
                               "mode": mode},
                    "row": row_index,
                    "cell": column_index,
                    "type": "{0}".format(type),
                    "value": "{0}".format(new_value)}
            response = self.server.post("command/core/edit-one-cell?project={0}".format(self.id), **{"data":data})
        except Exception as e:
            print "Request command/core/edit-one-cell?project={0} failed. {1}".format(self.id, e.message)

    def mass_edit(self, column_name, expression="value", edits=[], facets=[], mode="row-based"):
        """
        edits is of the form e.g.
        [{"to":"ALASKA", "from":["ANCHORAGE","FAIRBANKS","JUNEAU"]}]
        where edits must be an array and from must be an array
        """
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in facets],
                               "mode": mode},
                    "expression":Facet.prepare_expression(expression),
                    "columnName":column_name,
                    "edits":edits}
            response = self.server.post("command/core/mass-edit?project={0}".format(self.id), **{"data":data})
        except Exception as e:
            print "Request command/core/mass-edit?project={0} failed. {1}".format(self.id, e.message)


    def add_column(self, base_column, new_column, insert_index=0, expression="value", on_error="store-error", facets=[], mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in facets], "mode": mode}}
            response = self.server.post("command/core/add-column?baseColumnName={0}&expression={1}&newColumnName={2}&columnInsertIndex={3}&onError={4}&project={5}".format(
                RefineServer.simple_quote(base_column), RefineServer.simple_quote(expression),
                RefineServer.simple_quote(new_column), insert_index, RefineServer.simple_quote(on_error), self.id),
                                        **{"data": data})
            if TIMING: print "REFINE : adding column {0} took {1} seconds".format(new_column, time() - start)
        except Exception as e:
            print "Request command/core/add-column?baseColumnName={0}&expression={1}&newColumnName={2}&columnInsertIndex={3}&onError={4}&project={5}".format(
                RefineServer.simple_quote(base_column), RefineServer.simple_quote(expression),
                RefineServer.simple_quote(new_column), insert_index, RefineServer.simple_quote(on_error), self.id, e.message)


    def remove_column(self, column_name, facets=[], mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in facets], "mode": mode}}
            response = self.server.post("command/core/remove-column?columnName={0}&project={1}".format(RefineServer
                                                                                                       .simple_quote(column_name),
                                                                                                       self.id),
                                        **{"data": data})
            if TIMING: print "REFINE : removing column {0} took {1} seconds".format(column_name, time() - start)
        except Exception as e:
            print "Request command/core/remove-column?columnName={0}&project={1} failed. {2}".format(column_name,
                                                                                                     self.id, e.message)
            # response {"code":"ok","historyEntry":{"id":1364233915139,"description":"Remove column grouper","time":"2013-03-25T13:37:03Z"}}

    def remove_rows(self, facets=[], mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in facets], "mode": mode}}
            response = self.server.post("command/core/remove-rows?project={0}".format(self.id),
                                        **{"data": data})
            if TIMING: print "REFINE : removing rows took {0} seconds".format(time() - start)
        except http_exceptions.RequestException: print "Request command/core/remove-rows?project={0} failed.".format(
            self.id)

    def compute_facets(self, mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/compute-facets?project={0}".format(self.id),
                                        **{"data": data})
            if TIMING: print "REFINE : computing facets took {0} seconds".format(time() - start)
            try:
                json_response = response.json()
            except Exception as e:
                raise Exception("Failed to retrieve JSON of response to compute-facets request. {0}".format(e.message))
            return [FacetComputation(**f) for f in json_response.get("facets")]
        except http_exceptions.RequestException: print "Request command/core/compute-facets?project={0} failed.".format(
            self.id)

    def test_facets(self, test_facets, mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in test_facets], "mode": mode}}
            response = self.server.post("command/core/compute-facets?project={0}".format(self.id),
                                        **{"data": data})
            try:
                json_response = response.json()
            except Exception as e:
                raise Exception("Failed to retrieve JSON of response to compute-facets request. {0}".format(e.message))
            if TIMING: print "REFINE : testing facets took {0} seconds".format(time() - start)
            return [FacetComputation(**f) for f in json_response.get("facets")]
        except http_exceptions.RequestException: print "Request command/core/compute-facets?project={0} failed for test case.".format(
            self.id)


    def flag_row(self, row_number, state="true", mode="row-based"):
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/annotate-one-row?flagged={2}&row={0}&project={1}".format(
                row_number, self.id, state), **{"data": data})
        except http_exceptions.RequestException: print "Unable to flag row {0}.".format(row_number)

    def flag_rows(self, state="true", mode="row-based"):
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/annotate-rows?flagged={1}&project={0}".format(self.id, state),
                                        **{"data": data})
        except http_exceptions.RequestException: print "Unable to flag rows."

    def star_row(self, row_number, state="true", mode="row-based"):
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/annotate-one-row?starred={2}&row={0}&project={1}".format(
                row_number, self.id, state), **{"data": data})
        except http_exceptions.RequestException: print "Unable to star row {0}.".format(row_number)

    def star_rows(self, state="true", mode="row-based"):
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/annotate-rows?starred={1}&project={0}".format(self.id, state),
                                        **{"data": data})
        except http_exceptions.RequestException: print "Unable to star rows."

    def undo_redo(self, project_version=0, mode="row-based"):
        if TIMING: start = time()
        try:
            data = {"engine": {"facets": [f.refine_formatted_keys() for f in self.facets], "mode": mode}}
            response = self.server.post("command/core/undo-redo?lastDoneID={0}&project={1}".format(
                project_version, self.id), **{"data": data})
        except http_exceptions.RequestException: print "Unable to undo/redo to version {0}.".format(project_version)
        if TIMING: print "REFINE : undo/redo took {0} seconds".format(time() - start)



class HistoryEntry(object):
    def __init__(self, *args, **kwargs):
        self.id = kwargs.get("id", None)
        self.description = kwargs.get("description", None)
        self.time = kwargs.get("time", None)
        if self.time:
            self.time = datetime(int(self.time[0:4]), int(self.time[5:7]), int(self.time[8:10]),
                                 hour=int(self.time[11:13]), minute=int(self.time[14:16]), second=int(self.time[17:19]))

    def __unicode__(self):
        return "{0} {1} [ID:{2}]".format(self.time, self.description, self.id)

    def __str__(self):
        return self.__unicode__()


class SortCriterion(object):
    def __init__(self, column_name, column_type, reverse, blank_position=2, error_position=1, case_sensitive=False,
                 *args,
                 **kwargs):
        """
        column_type can be string, number, boolean, date
        string a-z default, z-a is "reverse"
        number smallest first is default, largest first is "reverse"
        date earliest first is default, latest first is "reverse"
        boolean false then true is default, true then false is "reverse"
        """

        if column_type == "string": self.case_sensitive = kwargs.get("case_sensitive", False)
        self.column = column_name
        self.value_type = column_type
        self.reverse = reverse
        self.blank_position = blank_position
        self.error_position = error_position
        self.case_sensitive = case_sensitive

    def __unicode__(self):
        return "Sort by {0} ({1}) {2}, with blank rows {3} and error rows {4}".format(self.column, self.value_type,
                                                                                      "descending" if self.reverse else "ascending",
                                                                                      "first" if self.blank_position == 0 else "last" if self.blank_position == 2 else "second",
                                                                                      "first" if self.error_position == 0 else "last" if self.error_position == 2 else "second")

    def __str__(self):
        return self.__unicode__()

    def refine_formatted_keys(self):
        key_formatted_repr = {}
        for k in self.__dict__.keys():
            new_key = "".join([c.capitalize() for c in k.split("_")])
            key_formatted_repr[new_key[0].lower() + new_key[1:]] = getattr(self, k)
        return key_formatted_repr


class Facet(object):

    def __init__(self, type=None, name="", column_name="", *args, **kwargs):
        self.type = kwargs.get("type", type)
        self.name = kwargs.get("name", name)
        self.column_name = kwargs.get("column_name", column_name)
        for k in kwargs.keys():
            setattr(self, k, kwargs.get(k, None))

    def __unicode__(self):
        return "{0} facet on {1}".format(self.type, self.column_name)

    def __str__(self):
        return self.__unicode__()

    def refine_formatted_keys(self):
        key_formatted_repr = {}
        for k in self.__dict__.keys():
            if k == "lower_bound": key_formatted_repr["from"] = getattr(self, k)
            elif k == "upper_bound": key_formatted_repr["to"] = getattr(self, k)
            else:
                new_key = "".join([c.capitalize() for c in k.split("_")])
                key_formatted_repr[new_key[0].lower() + new_key[1:]] = getattr(self, k)
        return key_formatted_repr

    @staticmethod
    def prepare_expression(base_expression):
        clean_expression = base_expression
        if not match("^(?:grel)|(?:jython)|(?:clojure):", clean_expression):
            clean_expression = "grel:" + clean_expression
        if DEBUG:
            print clean_expression
        return clean_expression


class ListFacet(Facet):
    def __init__(self, name="", column_name="", omit_blank=False, omit_error=False, selection=[], select_blank=False,
                 select_error=False, invert=False, expression="value", *args, **kwargs):
        """
        name and column name can be empty strings because facets can be applied to entire rows e.g. flagged
        expression is required
        selection is required
        """
        Facet.__init__(self, "list", name, column_name)
        self.expression = Facet.prepare_expression(kwargs.get("expression", expression))
        self.omit_blank = kwargs.get("omit_blank", omit_blank)
        self.omit_error = kwargs.get("omit_error", omit_error)
        self.selection = kwargs.get("selection", selection)
        # selection - v stands for value and l stands for label: [{"v":{"v":"video","l":"video"}}]
        self.select_blank = kwargs.get("select_blank", select_blank)
        self.select_error = kwargs.get("select_error", select_error)
        self.invert = kwargs.get("invert", invert)

    def __unicode__(self):
        return "List facet on {0} \"{1}\"".format(self.column_name, self.expression)

    def __str__(self):
        return self.__unicode__()


class RangeFacet(Facet):
    def __init__(self, name, column_name, lower_bound=None, upper_bound=None, select_numeric=True,
                 select_non_numeric=True, select_blank=True, select_error=True, expression="value", *args, **kwargs):
        """
        expression is required
        """
        Facet.__init__(self, "range", name, column_name)
        self.expression = Facet.prepare_expression(kwargs.get("expression", expression))
        self.select_numeric = kwargs.get("select_numeric", select_numeric)
        self.select_non_numeric = kwargs.get("select_non_numeric", select_non_numeric)
        self.select_blank = kwargs.get("select_blank", select_blank)
        self.select_error = kwargs.get("select_error", select_error)
        lb = kwargs.get("lower_bound", lower_bound)
        if lb: self.lower_bound = lb
        ub = kwargs.get("upper_bound", upper_bound)
        if ub: self.upper_bound = ub

    def __unicode__(self):
        return "Range facet on {0} \"{1}\"".format(self.column_name, self.expression)

    def __str__(self):
        return self.__unicode__()


class TimeRangeFacet(Facet):
    def __init__(self, name, column_name, lower_bound=None, upper_bound=None, select_time=True, select_non_time=True,
                 select_blank=True, select_error=True, expression="value", *args, **kwargs):
        """
        expression is required
        """
        Facet.__init__(self, "timerange", name, column_name)
        self.expression = Facet.prepare_expression(kwargs.get("expression", expression))
        self.select_time = kwargs.get("select_time", select_time)
        self.select_non_time = kwargs.get("select_non_time", select_non_time)
        self.select_blank = kwargs.get("select_blank", select_blank)
        self.select_error = kwargs.get("select_error", select_error)
        lb = kwargs.get("lower_bound", lower_bound)
        if lb: self.lower_bound = lb
        ub = kwargs.get("upper_bound", upper_bound)
        if ub: self.upper_bound = ub

    def __unicode__(self):
        return "Time range facet on {0} \"{1}\"".format(self.column_name, self.expression)

    def __str__(self):
        return self.__unicode__()


class TextFacet(Facet):
    def __init__(self, name, column_name, query=None, case_sensitive=False, *args, **kwargs):
        """
        case_sensitive is required
        mode is required
        """
        Facet.__init__(self, "text", name, column_name)
        self.case_sensitive = kwargs.get("case_sensitive", case_sensitive)
        self.query = RefineServer.simple_quote(kwargs.get("query", query))
        self.mode = "text"

    def __unicode__(self):
        return "Text facet on {0} \"{1}\"".format(self.column_name, self.query)

    def __str__(self):
        return self.__unicode__()


class FacetComputation(object):
    def __init__(self, *args, **kwargs):
        """
            a range/timerange facet will expose the following properties (in addition to repeating the properties that make up the facet):
                min - minimum value for all values in column
                max - maximum value for all values in column
                step - the step for the bins (below)
                bins - an array of counts for every stepped (above) value considering ALL facets
                base_bins - an array of counts for every stepped (above) value considering JUST THIS facet
                base_numeric_count - the total number of rows
                base_non_numeric_count - the total number of non-numeric cells in column
                base_blank_count - the total number of blank cells in column
                base_error_count - the total number of cells that errored on application of the facet
                numeric_count - the total number of cells resulting from ALL facets combined
                non_numeric_count - the count of non-numeric  cells in the column for JUST THIS facet
                blank_count - the count of blank cells for JUST THIS facet
                error_count - the count of error cells for JUST THIS facet

            a list facet will expose only one property, an array:
                choices
                    each entry in choices is a dict of the following form:
                        v: a dict with keys "v" for value and "l" for label
                        c: a count of the number of rows matching the above value for ALL facets combined
                        s: a Boolean indicating whether this value is selected as part of this facet (allowing for multi-select)

        """
        for k in kwargs.keys():
            quoted = kwargs.get(k, None)
            setattr(self, k, quoted)

    def __unicode__(self):
        return "\n".join(["{0}: {1}".format(k, str(getattr(self, k))) for k in self.__dict__.keys()])

    def __str__(self):
        return self.__unicode__()
