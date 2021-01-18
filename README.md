# gdrivefs-rust
Linux FUSE filesystem to emulate Google Drive File Stream

## What problem does it solve?
Some people (or companies) have really big Google Drives and the default client just syncs everything into a folder on your local disk. That's not necessarily bad since storage isn't too expensive anymore, but still, most of the files are ones that I rarely use, so why spend disk space on it?
So this is where this project comes into play: for Gsuite (Google Enterprise Stuff) Customers, they offer "Google Drive File Stream" which mounts your drive as a local folder, but doesn't sync it completely. 
It only downloads and caches often accessed files and removes them if you don't use them for some time. This means you can access everything in your drive like it's on your computer, but you only use a few megabytes of disk space.

So what's the problem when a solution exists? Well, first of all, File Stream is only for Gsuite customers, which makes it inaccessible for "normal" people using Google Drive. In addition, Google is still lacking a File Stream client for Linux, the current one only works on Windows and Mac. Since I'm using Linux at work, I thought I'd just write one. So here we are.

## What are the downsides?
As always, there's some trade-offs that we're making when using such a thing. First of all, it's *slow*. And by slow, I mean really slow. Of course, it depends on your network connection, but it's definitely much slower than your NVMe drive or SATA SSD. It will most likely even be slower than a regular HDD.

## How to use it?
**ATTENTION:** This tool is very much in early prototyping phase. Do NOT use it on any Drive that you have data on that you'll need. This tool might randomly corrupt or delete data or metadata, it's still far from stable.

Still there? Well, then let's start.

### 1. Create a Google API Client
To run the app, you currently need to create an API Key yourself. Don't worry, the Drive API is completely free. For that, you need a Google account. If you have that on hand, go to the [Google Cloud Console](https://console.developers.google.com/) and create or select a project.  
After that, select the Google Drive API in the [API Library](https://console.developers.google.com/apis/library/drive.googleapis.com?q=drive) and click "Activate". As soon as it's active, you can visit the [Drive API Overview](https://console.developers.google.com/apis/api/drive.googleapis.com/overview) where you can select "Credentials" and then create a new set of credentials.  
For the credentials type, you have to select "OAuth Client ID" and the application type will be "Desktop App". You can choose the name as you want. Now that you have valid credentials for the API, please click the Download button to the right to download the client credentials file. You need to place this as `clientCredentials.json` in the directory where you have your local copy of gdrivefs-rust.

### 2. Set-up Rust Nightly
We are using some nightly features of Rust in this project, so you need to switch to the nightly tools. With `rustup` installed, you can simply run `rustup override set nightly` in the project directory.

### 3. Run the app
You're almost ready. Use `cargo run` to run the application. On first launch, you will see a message in the console that tells you to open a link in your browser. Please do this, since this authenticates the application with your Google account. After authenticating, you either see the indexing start or the application crashes. If it crashes, see 4) since you might need to adjust some configuration properties.

### 4. Configure mount path
The default values of the configuration are not pretty sensible right now, so the default mount path might try to mount the filesystem somewhere you don't have permissions. To fix that, simply go to your configuration directory:

* MacOS: `/Users/<yourname>/Library/Application Support/StreamDrive`
* Linux: `/home/<youruser>/.config/StreamDrive`

If you have set your `XDG_CONFIG_HOME`, the configuration will be placed there instead.

Open the file `config.toml` and then edit the property `mount_path` in the `[general]` section. For development purposes, something under your home directory could be a sensible value.

## What already works
The basics. The application can index all **shared drives** you have access to. It can currently not work with "My Drive", though. When the filesystem is mounted, you can currently can:

* List files
* Create files
* Delete files
* Delete directories
* Write to files

> ***Note:*** Writing to files only works as long as the files are written as a whole. Seeking and writing in the middle of files isn't supported. So it mostly depends on the application you are using to write those files.

## What doesn't work
Biggest thing: performance. It's horrible. Really. In addition:

 * "My Drive"
 * Creating directories
 * Seeking in files/(re)writing only parts of a file
 * Local Caching
 * Graceful shutdown/unmounting
 * most likely much more that I currently don't know about, please report via issues if there's something missing for you :-)