__Summary__  
Shortly after Amazon Glacier was first announced, I started upgrading my backup
scripts to use it; I was previously using S3, and the price difference was
more than enough to convince me to switch.

However, at the time there were no tools available. So I started adding the
necessary features to my own script, until eventually it became a fairly
full-featured Glacier client in addition to an incremental backup program.

__Features__  
-Make incremental backups, 2 layers deep (full, partial, incremental)  
-Compress them with lrzip (external tool needed)  
-Automatically upload to Glacier  
-CLI-based upload, download, and list Glacier files  
-Track filename to Glacier key relations to allow downloading by file name  
-Multipart uploads for more reliable handling of large files

__Limitations__  
-Linux/UNIX oriented, should work fine in Cygwin though  
-Hardcoded external lrzip for compression, better if it were configurable  
-File hashes are stored by relative path, so cwd has to be the same  
-Inventory listing is done by printing the raw JSON, not too pretty  
-A lot of the output/progress messages could be prettier  
-Progress tracking for upload/download is by chunk, limitation of AWS library  
-No limitation of bandwidth usage, also limiation of AWS library  
-AWS library and all its dependencies are rather big and bloated

__Setup__  
-Install lrzip  
-Clone MaeBackup repository  
-cd to cloned repository  
-Run ./build.sh

__Usage__  
-Run maebackup.sh, usage details are included with the program
