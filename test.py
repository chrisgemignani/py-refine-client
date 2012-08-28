from refine_server import Facet, ListFacet, TextFacet, TimeRangeFacet, RangeFacet, Project, RefineServer



def testServerCapabilities():
    """
    Test that the server is running and has the right capabilities
    :return:
    """
    try:
        s = RefineServer()
    except Exception, e:
        raise

    assert s.version.full_name == 'Google Refine 2.5 [r2407]'
    server_configuration = s.configuration
    assert 'binary/xls' in [f.name for f in server_configuration.formats]
    assert 'application/vnd.ms-excel' in server_configuration.mime_types.keys()
    assert server_configuration.mime_types['application/vnd.ms-excel'] == 'binary/xls'


def testServerUpload():
    """
    Testing project creation using media.juiceanalytics.com CSV API...
    :return:
    """
    try:
        s = RefineServer()
        num_projects = len(s.projects)
        p = Project(url='http://demo.mediacore.tv/api/media?category=featured', name='testing', **{'recordPath':["__anonymous__","media","__anonymous__"]})
        p.transform_column("__anonymous__ - modified_on", "value.toDate()")
        comps = p.test_facets([TimeRangeFacet("__anonymous__ - modified_on", "__anonymous__ - modified_on", -6470126652719022000, -2305843009213694000)])
        assert len(s.projects) == (num_projects + 1)
        assert len(comps) == 1
    except Exception, e:
        raise
    finally:
        p.destroy()




def testServerUploadColumns():
    """
    Testing project creation using media.juiceanalytics.com CSV API...
    :return:
    """

    try:
        s = RefineServer()
        p = Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census2", separator=",")
        expecteds = ['STATE', 'SEX', 'AGE', 'POP2000', 'POP2008']
        for name, c in zip(expecteds, p.columns):
            assert name == c.name
    except Exception, e:
        raise
    finally:
        p.destroy()




def testServerFacets():
    """
    Testing project creation using media.juiceanalytics.com CSV API...
    :return:
    """

    try:
        s = RefineServer()
        p = Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census2", separator=",")

        assert len(p.facets) == 0

        # Insert facet test here...
        range_facet = RangeFacet("AGE", "AGE", 18, 21)
        data = p.rows(None, 5, 3)
        p.append_facet(range_facet)
        comps = p.compute_facets()
        assert len(p.facets) == 1
        assert comps[0].__dict__['baseNumericCount'] == 8772
        assert data.filtered_count == 8772

        list_facet = ListFacet("STATE", "STATE", False, False, [{"v":{"v":"Florida","l":"Florida"}}], True, True, False)
        data = p.rows(None, 5, 3)
        p.append_facet(list_facet)
        comps = p.compute_facets()
        assert len(p.facets) == 2
        assert data.filtered_count == 306

        text_facet = TextFacet("SEX", "SEX", "M")
        history = p.history
        data = p.rows(None, 5, 3)
        p.append_facet(text_facet)
        comps = p.compute_facets()
        assert data.total_count == 8772
        assert data.filtered_count == 6
        assert len(p.facets) == 3
        p.test_facets([RangeFacet("STATE", "STATE", **{"selectBlank": True, "selectError": True, "expression": "if(endsWith(value, \"%\"), value.replace(\"%\",\"\").replace(\",\",\"\"), \"NaN\").toNumber()", "selectNumeric": True})])
    except Exception, e:
        raise
    finally:
        p.destroy()


