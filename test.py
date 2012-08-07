from refine_server import *

s=RefineServer()
print s.version.full_name
print s.version.full_version
print s.version.revision
print s.version.version
for p in s.projects: print p.id
c = s.configuration
for f in c.formats: print f.name
print c.mime_types
print c.file_extensions
Project(url='http://demo.mediacore.tv/api/media?type=video', **{'recordPath':["__anonymous__","media","__anonymous__"]})
p=Project(url='http://demo.mediacore.tv/api/media?category=featured', name='testing', **{'recordPath':["__anonymous__","media","__anonymous__"]})
print p.facets
p.destroy()
p=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census")
p2=Project(url='http://media.juiceanalytics.com/census/CENSUS_STATEAGESEX.csv', name="census2", **{"separator":","})