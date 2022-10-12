# scholarly_sitemap
Makes or updates a Google Scholar compliant sitemap for the Islandora 7 xmlsitemap module.

Requires the xmlsitemap module. If the base module only created one xml file, change "if ($sitemap->chunks > 1) {" to "if ($sitemap->chunks > -1) {" on line 16 in xmlsitemap.pages.inc of the xmlsitemap module.

Makes a new sitemap in case the xmlsitemap module was activated and makes one base file (1.xml) with only the base website in it. Otherwise updates the current sitemap with values extracted from SOLR since the last time the sitemap was updated.
