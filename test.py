from refine_server import RangeFacet, Project, RefineServer


try:
    s=RefineServer()
except Exception, e: raise
print "\nThe server is running {0}\n".format(s.version.full_name)
project_list = s.projects
print "There are currently {0} projects:\n".format(len(project_list))
for p in project_list: print p.id
server_configuration = s.configuration
print "--Server Configuration--\n\nFormats:\n"
for f in server_configuration.formats: print f.name
print "MIME Types:\n"
print server_configuration.mime_types
print "File Extensions:\n"
print server_configuration.file_extensions
print "Testing project creation using demo.mediacore.tv JSON API..."
try: p=Project(url='http://demo.mediacore.tv/api/media?category=featured', name='testing', **{'recordPath':["__anonymous__","media","__anonymous__"]})
except Exception, e: raise
print "Testing project creation using media.juiceanalytics.com CSV API..."
try:
    p2=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census")
    print "Columns comprise:\n"
    for o in p2.columns: print "{0}: {1}".format(o.cell_index, o.name)
    p2.split_column_by_separator(p2.column_names[0], ",")
    print "Columns comprise:\n"
    for o in p2.columns: print "{0}: {1}".format(o.cell_index, o.name)
    "Destroying project."
    p2.destroy()
except Exception, e: raise
try:
    p3=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census2", **{"separator":","})
    print "Columns comprise:\n"
    for o in p3._columns: print "{0}: {1}".format(o.cell_index, o.name)
    "Project has {0} facets:\n".format(len(p3.facets))
    print p3.facets
    # Insert facet test here...
    range_facet = RangeFacet("test", p3.column_names[2], 18, 21)
    p3.append_facet(range_facet)
    data = p3.rows(None, 5, 3)
    print "Of {0} total rows {1} were returned after applying facets.".format(data.total_count, data.filtered_count)
    print "Sample row:\n{0}\n".format(data.rows[0])
    p3.compute_facets()
    "Project has {0} facets:\n".format(len(p3.facets))
    print p3.facets
    "Destroying project."
    p3.destroy()
except Exception, e: raise