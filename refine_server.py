from requests import get as http_get, post as http_post, codes as http_codes, exceptions as http_exceptions
from urllib import quote, quote_plus
from random import randint
from re import match
try: import simplejson as json
except ImportError: import json
from os.path import basename
from time import sleep
from mimetools import choose_boundary
from datetime import datetime
from re import IGNORECASE, search, sub


DEBUG = False
DEFAULT_MIME_TYPE = "text/csv"

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
			self.formats = [RefineFormat(name=k, **kwargs['formats'][k])\
			                for k in kwargs['formats'].keys()]
			self.mime_types = kwargs['mimeTypeToFormat']
			self.file_extensions = kwargs['extensionToFormat']
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
		self.full_name = kwargs.get('full_name', full_name)
		self.full_version = kwargs.get('full_version', full_version)
		self.revision = kwargs.get('revision', revision)
		self.version = kwargs.get('version', version)

	def __unicode__(self):
		return self.full_version

	def __str__(self):
		return self.__unicode__()


class RefineServer(object):
	"""
		Docstring
		"""

	def __init__(self, protocol='http', host='127.0.0.1', port='3333', *args, **kwargs):
		self.protocol = kwargs.get('protocol', protocol)
		self.host = kwargs.get('host', host)
		self.port = kwargs.get('port', port)

	def __unicode__(self):
		return "{0}://{1}:{2}".format(self.protocol, self.host, self.port)

	def __str__(self):
		return self.__unicode__()

	def get(self, action):
		if DEBUG:
			print "REQUEST URL : {0}".format(str(action))
		try:
			return http_get("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action))
		except http_exceptions.RequestException:
			raise

	def post(self, action, data=None, headers=None, files=None, **kwargs):
		if DEBUG:
			print "REQUEST URL : {0}\nDATA : {1}\nHEADERS : {2}".format(str(action), str(data), str(headers))
		try:
			new_kwargs = {"data": kwargs.get("data", data),
			              "files": kwargs.get("files", files),
			              "headers": kwargs.get("headers", headers)}
			response = http_post("{0}://{1}:{2}/{3}".format(self.protocol, self.host, self.port, action), **new_kwargs)
			if DEBUG and action.find("get-rows") == -1:
				try:
					print(("RESPONSE : {0}").format(response.text))
				except Exception as e:
					print e
			return response
		except http_exceptions.RequestException: print "Request {0} failed.".format(action)

	def destroy_all_projects(self):
		for p in self.projects:
			if DEBUG:
				print "Destroying {0}".format(p.id)
			p.destroy()

	@property
	def version(self):
		response = self.get("command/core/get-version")
		if response.status_code == http_codes.ok: return RefineVersion(**response.json)

	@property
	def projects(self):
		response = self.get("command/core/get-all-project-metadata")
		if response.status_code == http_codes.ok:
			return [Project(id=pid) for pid in response.json['projects'].keys()]
		else:
			print "Request command/core/get-all-project-metadata failed."

	@property
	def configuration(self):
		response = self.post("command/core/get-importing-configuration")
		if response.status_code == http_codes.ok:
			return RefineConfiguration(**response.json['config'])
		else:
			print "Request command/core/get-importing-configuration failed."


class DataSource(object):
	"""
		Docstring
		"""

	def __init__(self, declared_mime_type=None, location=None, file_name=None, origin=None, url=None,
	             size=None, *args, **kwargs):
		self.declared_mime_type = kwargs.get('declaredMimeType', declared_mime_type)
		self.location = kwargs.get('location', location)
		self.fileName = kwargs.get('fileName', file_name)
		self.origin = kwargs.get('origin', origin)
		self.url = kwargs.get('url', url)
		self.size = kwargs.get('size', size)

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
		self.files = [DataSource(**f) for f in kwargs.get('files', files)]
		self.download_count = kwargs.get('downloadCount', download_count)
		self.archive_count = kwargs.get('archiveCount', archive_count)
		self.clipboard_count = kwargs.get('clipboardCount', clipboard_count)
		self.upload_count = kwargs.get('uploadCount', upload_count)

	def __unicode__(self):
		return ("{0} downloads, {1} archives, "
		        "{2} clipboards, {3} uploads\n{4}").format(self.download_count, self.archive_count,
			self.clipboard_count, self.upload_count, str(self.files))

	def __str__(self):
		return self.__unicode__()


class ImportJobDetails():
	def __init__(self, ranked_formats=None, has_data=None, state=None, file_selection=None,
	             retrieval_record=None, *args, **kwargs):
		self.ranked_formats = kwargs.get('rankedFormats',
			ranked_formats) # array of mime types in order of best guess for this data source
		self.has_data = kwargs.get('hasData', has_data)
		self.state = kwargs.get('state', state)
		self.file_selection = kwargs.get('fileSelection',
			file_selection) # an array of indices that correspond to values in retrievalRecord["files"]
		if retrieval_record or "retrievalRecord" in kwargs:
			self.retrieval_record = RetrievalRecord(**kwargs.get('retrievalRecord', retrieval_record))
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
		        "total rows ({3} filtered rows)\nSample Row:\n".format(self.limit, self.offset, self.total_count,
			self.filtered_count, str(self.rows[0])))

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
		self.id = kwargs.get('id', id)
		self._facets = []
		self._sort_critieria = []
		self._columns = []
		self.row_set = None
		if not self.id:
			job_id = self._fetch_new_job()
			if path or "path" in kwargs:
				self._create_project_from_file(kwargs.get("path", path), job_id, kwargs.get("name", name), **kwargs)
			elif url or "url" in kwargs:
				self._create_project_from_url(kwargs.get("url", url), job_id, kwargs.get("name", name), **kwargs)

	def __unicode__(self):
		return "Project ID {0} using server at {1}".format(self.id, unicode(self.server))

	def __str__(self):
		return self.__unicode__()

	def destroy(self):
		if self.id:
			response = self.server.post("command/core/delete-project", **{"data": {"project": self.id}})
			if response and response.json["code"] != "ok":
				print "Request command/core/delete-project failed."# placeholder - do something if it fails?

	def _fetch_new_job(self):
		response = None
		try: response = self.server.post("command/core/create-importing-job")
		except http_exceptions.RequestException: print "Request command/core/create-importing-job failed."
		if response: return response.json["jobID"]

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
		self._facets.clear()

	@property
	def name(self):
		return self._name

	def rename_column(self, old_name, new_name):
		try: response = self.server.post(("/command/core/rename-column?oldColumnName={0}"
		                                  "&newColumnName={1}&project={2}".format(old_name, new_name, self.id)))
		except http_exceptions.RequestException: print("Request /command/core/rename-column?oldColumnName={0}"
		                                               "&newColumnName={1}&project={2}".format(old_name, new_name, self.id))

	@property
	def sort_criteria(self):
		return self._sort_critieria

	@sort_criteria.setter
	def sort_criteria(self, sort_criterion=None):
		"""
				since the sort criteria are stored on the client side, passing in a sort criterion
				adds to the list of saved criteria and passing in a null value clears the previously
				saved criteria
				sort_criterion should be of type SortCriterion
				"""

		if sort_criterion: self._sort_critieria.append(sort_criterion)
		else: self._sort_critieria.clear()

	@property
	def history(self):
		try: response = self.server.get("command/core/get-history?project={0}".format(self.id))
		except http_exceptions.RequestException: print "Request command/core/get-history?project={0} failed.".format(
			self.id)
		if response: return [HistoryEntry(**h) for h in response.json["past"]], [HistoryEntry(**h) for h in
		                                                                         response.json["future"]]

	@property
	def processes(self):
		try: response = self.server.post("command/core/get-processes?project={0}".format(self.id))
		except http_exceptions.RequestException: print "Request command/core/get-processes?project={0} failed.".format(
			self.id)
		if response: return response.json["processes"]

	@property
	def metadata(self):
		try: return self.server.post("command/core/get-project-metadata?project={0}".format(self.id))
		except http_exceptions.RequestException: print "Request command/core/get-project-metadata?project={0}".format(
			self.id)

	@property
	def column_names(self):
		return map(lambda c: c.name, self._columns)

	@property
	def columns(self):
		self._fetch_models()
		return self._columns

	def rows(self, job_id=None, offset=0, limit=-1, mode="row-based"):
		try:
			if job_id:
				response = self.server.post(
					"command/core/get-rows?importingJobID={0}&start={1}&limit={2}".format(job_id, offset, limit),
					**{"data": {"callback": "jsonp{0}".format(randint(1000000000000, 1999999999999))}})
			else:
				callback = "jsonp{0}".format(randint(1000000000000, 1999999999999))
				response = self.server.post(("command/core/get-rows?project={0}&start={1}&limit={2}"
				                             "&callback={3}".format(self.id, offset, limit, callback)),
					**{"data": "engine={0}&sorting={1}&callback={2}".format(
						json.dumps({"facets": [f.refine_formatted() for f in self.facets], "mode": mode}),
						json.dumps({"criteria": [s.refine_formatted() for s in self.sort_criteria]}), callback)})
		except Exception: print "Unable to retrieve rows."
		if response:
			response = json.loads(response.text[19:-1])
			self.row_set = RowSet(offset, limit, response["filtered"], response["total"], response["rows"])
			return self.row_set
		else: return None

	def export(self, filename, template, format="template", prefix="{\"rows\":[", suffix="]}",
	           separator=",", sorting=[], facets=[], mode="row-based", *args, **kwargs):
		data = {"project": self.id,
		        "engine": json.dumps({"facets": [f.refine_formatted() for f in kwargs.get("facets", facets)],
		                              "mode": kwargs.get("mode", mode)}),
		        "template": kwargs.get("template", template),
		        "format": kwargs.get("format", format),
		        "prefix": kwargs.get("prefix", prefix),
		        "suffix": kwargs.get("suffix", suffix),
		        "sorting": json.dumps({"criteria": [s.refine_formatted() for s in kwargs.get("sorting", sorting)]}),
		        "separator": kwargs.get("separator", separator)}
		try:
			response = self.server.post("command/core/export-rows/{0}".format(filename), **{"data": data})
		except http_exceptions.RequestException: print "Failed to export rows."
		return response.json

	def transform_column(self, column_name, grel_expression, on_error="keep-original", repeat=False, repeat_count=1):
		"""
				on_error options: keep-original, set-to-blank, store-error
				repeat default is false but can be set to true in which case repeat_count should be set to the number of iterations
				"""

		try:
			response = self.server.post(
				"command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(
					column_name, grel_expression, on_error, repeat, repeat_count, self.id))
		except http_exceptions.RequestException:
			print "Request command/core/text-transform?columnName={0}&expression={1}&onError={2}&repeat={3}&repeatCount={4}&project={5}".format(
				column_name, grel_expression, on_error, repeat, repeat_count, self.id)

	def _get_import_job_status(self, job_id):
		response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
		job_status = None
		if response and response.json.get("status") == "error":
			print("Request command/core/get-importing-job-status?"
			      "jobID={0} returned with error.\n{1}\n{2}".format(job_id,
				response.json["job"]["config"]["error"],
				response.json["job"]["config"]["errorDetails"]))
		elif response:
			if response.json["job"]["config"]["state"] == "error":
				print("Request command/core/get-importing-job-status?"
				      "jobID={0} returned with error.\n{1}\n{2}".format(job_id,
					response.json["job"]["config"]["error"],
					response.json["job"]["config"]["errorDetails"]))
			job_status = ImportJobDetails(**response.json["job"]["config"])
			while job_status.state != "ready" and job_status.state != "created-project":
				sleep(1)
				try:
					response = self.server.post("command/core/get-importing-job-status?jobID={0}".format(job_id))
					job_status = ImportJobDetails(**response.json["job"]["config"])
				except Exception:
					print "Request command/core/get-importing-job-status?jobID={0} failed.".format(job_id)
					break
			if job_status.state == "created-project":
				self.id = response.json["job"]["config"]["projectID"]
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
			print "Initializing parser to {0}".format
		try:
			response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
			                             "&jobID={0}&subCommand=initialize-parser-ui&format={1}".format(job_id,
				quote_plus(format))))
			if DEBUG:
				print "Initialize parser : {0}".format(response.text)
			return response.json["options"]
		except Exception:
			print "Failed to initialize-parser-ui."
			return dict()

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
				'storeBlankRows': kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", True)),
				'ignoreLines': kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
				"sheets": kwargs.get("sheets", [0]),
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'xmlBased': kwargs.get("xml_based", kwargs.get("xmlBased", True)),
				'storeBlankCellsAsNulls': kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
				'headerLines': kwargs.get("header_lines", kwargs.get("headerLines", 1)),
				'limit': kwargs.get("limit", -1)
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
				'encoding': kwargs.get("encoding", ""),
				'recordPath': kwargs.get("recordPath", kwargs.get("record_path", None)),
				'linesPerRow': kwargs.get("lines_per_row", kwargs.get("linesPerRow", 1)),
				'limit': kwargs.get("limit", -1),
				'separator': kwargs.get("separator", None),
				'ignoreLines': kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
				'headerLines': kwargs.get("header_lines", kwargs.get("headerLines", 0)),
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'storeBlankRows': kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", True)),
				'guessCellValueTypes': kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", False)),
				'processQuotes': kwargs.get("process_quotes", kwargs.get("processQuotes", False)),
				'storeBlankCellsAsNulls': kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False))
			},
			"text/line-based/*sv": {
				'processQuotes': kwargs.get("process_quotes", kwargs.get("processQuotes", True)),
				'storeBlankRows': kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", True)),
				'ignoreLines': kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'separator': kwargs.get("separator", u'\\t'),
				'storeBlankCellsAsNulls': kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
				'guessCellValueTypes': kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", True)),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
				'headerLines': kwargs.get("header_lines", kwargs.get("headerLines", 1)),
				},
			"text/xml/rdf": {
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
				'encoding': kwargs.get("encoding", "")
			},
			"text/line-based/fixed-width": {
				'storeBlankRows': kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", True)),
				'ignoreLines': kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'storeBlankCellsAsNulls': kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
				'headerLines': kwargs.get("header_lines", kwargs.get("headerLines", 1)),
				'encoding': kwargs.get("encoding", ""),
				'columnWidths': kwargs.get("column_widths", kwargs.get("columnWidths", None)),
				'columnNames': kwargs.get("column_names", kwargs.get("columnNames", None)),
				'limit': kwargs.get("limit", -1),
				'guessCellValueTypes': kwargs.get("guess_cell_value_types", kwargs.get("guessCellValueTypes", False))
			},
			"text/line-based/pc-axis": {
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'limit': kwargs.get("limit", -1),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False))
			},
			"text/xml/ods": {
				'sheets': kwargs.get("sheets", [0]),
				'limit': kwargs.get("limit", -1),
				'storeBlankRows': kwargs.get("store_blank_rows", kwargs.get("storeBlankRows", True)),
				'ignoreLines': kwargs.get("ignore_lines", kwargs.get("ignoreLines", -1)),
				'sheetRecords': kwargs.get("sheet_records", kwargs.get("sheetRecords", [])),
				'skipDataLines': kwargs.get("skip_data_lines", kwargs.get("skipDataLines", 0)),
				'storeBlankCellsAsNulls': kwargs.get("store_blank_cells_as_nulls", kwargs.get("storeBlankCellsAsNulls", True)),
				'includeFileSources': kwargs.get("include_file_sources", kwargs.get("includeFileSources", False)),
				'headerLines': kwargs.get("header_lines", kwargs.get("headerLines", 1))
			}
		}
		self.create_options = data_options[refine_mime_type]
		try:
			headers = {'content-type': 'application/x-www-form-urlencoded'}
			return self.server.post(("command/core/importing-controller?controller=core/default-importing-controller"
			                         "&jobID={0}&subCommand=update-format-and-options".format(job_id)),
				**{"headers": headers,
				   "data": "format={0}&options={1}".format(quote_plus(refine_mime_type),
					   json.dumps(self.create_options))})
		except Exception: print "Error updating format."

	def _fetch_models(self, job_id=None):
		try:
			if job_id: response = self.server.post("command/core/get-models?importingJobID={0}".format(job_id)).json
			else: response = self.server.post("command/core/get-models?project={0}".format(self.id)).json
		except Exception: print "Unable to retrieve model definitions."
		if response.get("columnModel", None) and response["columnModel"].get("columns", None):
			self._columns = [ColumnDefinition(**c) for c in response["columnModel"]["columns"]]
			self._columns.sort(key=lambda i: i.cell_index)
		if DEBUG: print self._columns

	def _create(self, job_id, mime_type, name="default", **kwargs):
		try:
			headers = {'content-type': 'application/x-www-form-urlencoded'}
			self.create_options["projectName"] = name
			data = "format={0}&options={1}".format(quote_plus(mime_type), quote_plus(json.dumps(self.create_options)))
			response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
			                             "&jobID={0}&subCommand=create-project".format(job_id)),
				**{"data": data, "headers": headers})
			# _get_import_job_status returns more information with each successive request involved in the project creation process
			# after create-project it shows the projectID and is the only way of discovering this value given a job id
			job_status = self._get_import_job_status(job_id)
			return response
		except http_exceptions.RequestException: raise

	def _create_project_from_file(self, path, job_id, name, **kwargs):
		files = {'file': (basename(path), open(path, 'rb'))}
		response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
		                             "&jobID={0}&subCommand=load-raw-data".format(job_id)), **{"files": files})
		if response and response.json:
			print "Failed to load data source {0}. ".format(path) + response.json # error message
		job_status = self._get_import_job_status(job_id) # polls for import completion

		if DEBUG:
			print job_status

		mime_type = job_status.ranked_formats[0]
		if mime_type=="text/json" and not kwargs.has_key("record_path") and not kwargs.has_key("recordPath"):
			kwargs["recordPath"] = Project.identify_json_record_path(None, path)
		elif mime_type=="text/line-based/*sv" and not kwargs.has_key("separator"):
			kwargs["separator"]=Project.sv_separator(None, path)

		presets = self._initialize_parser(job_id, mime_type)
		presets.update(kwargs)
		update_response = self._update_format(job_id, mime_type, **presets)
		if DEBUG:
			print "Presets : {0}".format(presets)
		self._fetch_models(job_id)
		self._create(job_id, mime_type, name, **kwargs)

	def _create_project_from_url(self, url, job_id, name, **kwargs):
		mime_type = http_get(url).headers["content-type"]
		if mime_type.find(";") > 0:
			mime_type = mime_type[0:mime_type.find(";")]
		if DEBUG:
			print "Provided MIME Type : {0}".format(mime_type)
		try:
			mime_type = self.server.configuration.mime_types[mime_type]
		except Exception:
			if mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
				mime_type = "text/xml/xlsx"
		if mime_type=="text/json" and not kwargs.has_key("record_path") and not kwargs.has_key(
			"recordPath"):
			kwargs["recordPath"] = Project.identify_json_record_path(url)
		elif mime_type=="text/line-based/*sv" and not kwargs.has_key("separator"):
			kwargs["separator"]=Project.sv_separator(url)
		boundary = choose_boundary()
		data = "--{0}\r\nContent-Disposition: form-data; name=\"download\"\r\n\r\n{1}\r\n--{0}--".format(boundary, url)
		headers = {"content-type": "multipart/form-data; boundary={0}".format(boundary)}
		response = self.server.post(("command/core/importing-controller?controller=core%2Fdefault-importing-controller"
		                             "&jobID={0}&subCommand=load-raw-data".format(job_id)),
			**{"data": data, "headers": headers})
		if response and response.json:
			print "Failed to load data source {0}. ".format(url) + response.json
		job_status = self._get_import_job_status(job_id) # polls for import completion
		if DEBUG:
			print "Running with MIME Type : {0}".format(mime_type)
		if mime_type:
			presets = self._initialize_parser(job_id, mime_type)
		presets.update(kwargs)
		update_response = self._update_format(job_id, mime_type, **presets)
		self._fetch_models(job_id)
		self._create(job_id, mime_type, name, **presets)

	@staticmethod
	def sv_separator(url=None, path=None):
		if url:
			content = http_get(url).text

		elif path:
			content = file.open(path,'r').readlines(1000)

		if match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|,)+$", content):
			return ","
		elif match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|\t)+$", content):
			return u'\\t'
		elif match("^(?:(?:\"[^\"]+\")|(?:[^,]+)|;)+$", content):
			return ";"

	@staticmethod
	def identify_json_record_path(url=None, path=None):
		if url:
			content = http_get(url).json
		elif path:
			content = json.loads(file.open(path,'r').read())
		path = []

		# Note that google refine's expects a nameless node to be specified as "__anonymous__", e.g. the root node
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

	def split_multi_value_cell(self, column, key_column, separator):
		try:
			response = self.server.post(("command/core/split-multi-value-cells?columnName={0}&keyColumnName={1}"
			                             "&separator={2}&mode=plain&project={3}".format(column, key_column,
				separator, self.id)))
		except http_exceptions.RequestException: print "Unable to split cell."

	def split_column_by_separator(self, column, separator=",", regex=False, remove_original=True, guess_cell_type=True):
		try:
			response = self.server.post(("command/core/split-column?columnName={0}&mode=separator&project={1}"
			                             "&guessCellType={2}&removeOriginalColumn={3}&separator={4}"
			                             "&regex={5}".format(column, self.id, str(guess_cell_type).lower(),
				str(remove_original).lower(), quote_plus(separator),
				quote_plus(regex) if regex else "false")))
			self._fetch_models()
		except http_exceptions.RequestException: print "Unable to split column."

	def split_column_by_field_length(self, column, lengths, remove_original=True, guess_cell_type=True):
		try:
			response = self.server.post(("command/core/split-column?columnName={0}&mode=lengths&project={1}"
			                             "&guessCellType={2}&removeOriginalColumn={3}"
			                             "&fieldLengths={4}".format(column, self.id, str(guess_cell_type).lower(),
				str(remove_original).lower(), quote_plus(lengths))))
			self._fetch_models()
		except http_exceptions.RequestException: print "Unable to split column."
		return response

	def compute_facets(self, mode="row-based"):
		try:
			response = self.server.post("command/core/compute-facets?project={0}".format(self.id),
				**{
					"data": "engine={0}".format(
						json.dumps({"facets": [f.refine_formatted() for f in self.facets], "mode": mode}))})
			return [FacetComputation(**f) for f in response.json["facets"]]
		except http_exceptions.RequestException: print "Request command/core/compute-facets?project={0} failed.".format(
			self.id)

	def test_facets(self, test_facets, mode="row-based"):
		try:
			response = self.server.post("command/core/compute-facets?project={0}".format(self.id),
				**{
					"data": "engine={0}".format(
						json.dumps({"facets": [f.refine_formatted() for f in test_facets], "mode": mode}))})
			return [FacetComputation(**f) for f in response.json["facets"]]
		except http_exceptions.RequestException: print "Request command/core/compute-facets?project={0} failed for test case.".format(
			self.id)

	def roll_to_history_entry(self, history_entry, mode="row-based"):
		try: return self.server.post("command/core/undo-redo?lastDoneID={0}&project={1}".format(history_entry, self.id),
			**{
				"data": {
					"engine={0}".format(json.dumps({"mode": mode, "facets": [f.refine_formatted() for f in self.facets]}))}})
		except http_exceptions.RequestException: print "Unable to go to history entry."


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
	def __init__(self, column_name, column_type, reverse, blank_position=2, error_position=1, *args, **kwargs):
		"""
				column_type can be string, number, boolean, date
				string a-z default, z-a is "reverse"
				number smallest first is default, largest first is "reverse"
				date earliest first is default, latest first is "reverse"
				boolean false then true is default, true then false is "reverse"
				"""

		if column_type == "string": self.case_sensitive = kwargs.get("case_sensitive", False)
		self.column_name = column_name
		self.column_type = column_type
		self.reverse = reverse
		self.blank_position = blank_position
		self.error_position = error_position

	def __unicode__(self):
		return "Sort by {0} ({1}) {2}, with blank rows {3} and error rows {4}".format(self.column_name, self.column_type,
			"descending" if self.reverse else "ascending",
			"first" if self.blank_position == 0 else "last" if self.blank_position == 2 else "second",
			"first" if self.error_position == 0 else "last" if self.error_position == 2 else "second")

	def __str__(self):
		return self.__unicode__()

	def refine_formatted(self):
		key_formatted_repr = {}
		for k in self.__dict__.keys():
			new_key = "".join([c.capitalize() for c in k.split("_")])
			key_formatted_repr[new_key[0].lower() + new_key[1:]] = getattr(self, k)
		return key_formatted_repr


class Facet(object):
	def __init__(self, type, name, column_name, *args, **kwargs):
		self.type = kwargs.get("type", type)
		self.name = kwargs.get("name", name)
		self.column_name = kwargs.get("column_name", column_name)
		for k in kwargs.keys(): setattr(self, k, kwargs.get(k, None))

	def __unicode__(self):
		return "{0} facet on {1}".format(self.type, self.column_name)

	def __str__(self):
		return self.__unicode__()

	def refine_formatted(self):
		key_formatted_repr = {}
		for k in self.__dict__.keys():
			if k == "lower_bound": key_formatted_repr["from"] = getattr(self, k)
			elif k == "upper_bound": key_formatted_repr["to"] = getattr(self, k)
			else:
				new_key = "".join([c.capitalize() for c in k.split("_")])
				key_formatted_repr[new_key[0].lower() + new_key[1:]] = getattr(self, k)
		return key_formatted_repr


class ListFacet(Facet):
	def __init__(self, name, column_name, omit_blank=False, omit_error=False, selection=[], select_blank=True,
	             select_error=True, invert=False, expression="value", *args, **kwargs):
		"""
				expression is required
				selection is required
				"""
		Facet.__init__(self, "list", name, column_name)
		self.expression = ("grel:" + kwargs.get("expression", expression)).replace("%", quote("%"))
		self.omit_blank = kwargs.get("omit_blank", omit_blank)
		self.omit_error = kwargs.get("omit_error", omit_error)
		self.selection = kwargs.get("selection",
			selection) # v stands for value and l stands for label: [{"v":{"v":"video","l":"video"}}]
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
		self.expression = ("grel:" + kwargs.get("expression", expression)).replace("%", quote("%"))
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
		self.expression = ("grel:" + kwargs.get("expression", expression)).replace("%", quote("%"))
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
		self.query = (kwargs.get("query", query)).replace("%", quote("%"))
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
		for k in kwargs.keys(): setattr(self, k, kwargs.get(k, None))

	def __unicode__(self):
		return "\n".join(["{0}: {1}".format(k, getattr(self, k)) for k in self.__dict__.keys()])

	def __str__(self):
		return self.__unicode__()