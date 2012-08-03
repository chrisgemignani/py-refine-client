from refine_server import *

s=RefineServer()
s.version.full_name
s.version.full_version
s.version.revision
s.version.version
for p in s.projects: print p.id
c = s.configuration
for f in c.formats: print f.name
c.mime_types
c.file_extensions
Project(url='http://demo.mediacore.tv/api/media?type=video', **{'recordPath':["__anonymous__","media","__anonymous__"]})
Project(url='http://demo.mediacore.tv/api/media?category=featured', **{'recordPath':["__anonymous__","media","__anonymous__"]})