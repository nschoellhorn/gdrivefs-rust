# gdrivefs-rust
Linux FUSE filesystem to emulate Google Drive File Stream

### What problem does it solve?
Some people (or companies) have really big Google Drives and the default client just syncs everything into a folder on your local disk. That's not necessarily bad since storage isn't too expensive anymore, but still, most of the files are ones that I rarely use, so why spend disk space on it?
So this is where this project comes into play: for Gsuite (Google Enterprise Stuff) Customers, they offer "Google Drive File Stream" which mounts your drive as a local folder, but doesn't sync it completely. 
It only downloads and caches often accessed files and removes them if you don't use them for some time. This means you can access everything in your drive like it's on your computer, but you only use a few megabytes of disk space.

So what's the problem when a solution exists? Well, first of all, File Stream is only for Gsuite customers, which makes it inaccessible for "normal" people using Google Drive. In addition, Google is still lacking a File Stream client for Linux, the current one only works on Windows and Mac. Since I'm using Linux at work, I thought I'd just write one. So here we are.

### What are the downsides?
As always, there's some trade-offs that we're making when using such a thing. First of all, it's *slow*. And by slow, I mean really slow. Of course, it depends on your network connection, but it's definitely much slower than your NVMe drive or SATA SSD. It will most likely even be slower than a regular HDD.

### How to use it?
**ATTENTION:** This tool is very much in early prototyping phase. Do NOT use it on any Drive that you have data on that you'll need. This tool might randomly corrupt or delete data or metadata, it's still far from stable.

Still there? Well, then let's start.

#### 1. Create a Google API Client

#### 2. Set Rust nightly
