from refine_server import Facet, ListFacet, TextFacet, TimeRangeFacet, RangeFacet, Project, RefineServer


try:
    s=RefineServer()
except Exception, e: raise
print s
project_list = s.projects
print "\n\n{0} Projects\n".format(len(project_list))
for p in project_list: print p.id
print "\n\nServer Configuration:\n"
print s.configuration
print "\n\nTesting project creation using demo.mediacore.tv JSON API..."
try:
    p=Project(url='http://demo.mediacore.tv/api/media?category=featured', name='testing', **{'recordPath':["__anonymous__","media","__anonymous__"]})
    p.transform_column("__anonymous__ - modified_on", "value.toDate()")
    facet = TimeRangeFacet("__anonymous__ - modified_on", "__anonymous__ - modified_on", -6470126652719022000, -2305843009213694000)
    print facet
    result = p.test_facets([facet])
    print "\n\nComputation Results:\n"
    for r in result: print r
    p.destroy()
except Exception, e: raise
print "\n\nTesting project creation using media.juiceanalytics.com CSV API..."
try:
    p2=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census")
    print "\n\nColumns comprise:\n"
    for o in p2.columns: print "{0}: {1}".format(o.cell_index, o.name)
    print "\n\nSplitting columns by comma."
    p2.split_column_by_separator(p2.column_names[0], ",")
    print "\n\nColumns comprise:\n"
    for o in p2.columns: print "{0}: {1}".format(o.cell_index, o.name)
    print "\n\nDestroying project."
    p2.destroy()
except Exception, e: raise
try:
    p3=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census2", **{"separator":","})
    print "\n\nColumns comprise:\n"
    for o in p3._columns: print "{0}: {1}".format(o.cell_index, o.name)
    print "\n\nProject has {0} facets:\n".format(len(p3.facets))
    for f in p3.facets: print f   # Insert facet test here...
    range_facet = RangeFacet("AGE", "AGE", 18, 21)
    for h in p3.history: print h
    print p3.rows(None, 5, 3)
    print "\n\nAdding {0}".format(range_facet)
    p3.append_facet(range_facet)
    result = p3.compute_facets()
    print "\n\nComputation Results:\n"
    for r in result: print unicode(r)
    list_facet = ListFacet("STATE", "STATE", False, False, [{"v":{"v":"Florida","l":"Florida"}}], True, True, False)
    for h in p3.history: print h
    print p3.rows(None, 5, 3)
    print "\n\nAdding {0}".format(list_facet)
    p3.append_facet(list_facet)
    result = p3.compute_facets()
    print "\n\nComputation Results:\n"
    for r in result: print r
    text_facet = TextFacet("SEX", "SEX", "M")
    for h in p3.history: print h
    print p3.rows(None, 5, 3)
    print "\n\nAdding {0}".format(text_facet)
    p3.append_facet(text_facet)
    result = p3.compute_facets()
    print "\n\nComputation Results:\n"
    for r in result: print r
    print "\n\nProject has {0} facets:\n".format(len(p3.facets))
    for f in p3.facets: print f
    print "\n\nDestroying project.\n"
    p3.destroy()
except Exception, e: raise