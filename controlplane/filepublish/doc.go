// Package filepublish is the file-store counterpart of an S3 bucket for [s3publish.Publisher]: a [Store] implements
// [s3publish.API] over a directory, so the upload protocol (objects first, manifest last, conditional write of the
// manifest) is the one of S3 and is not copied.
//
// Keys map to paths below the root exactly as the reader of a file connection maps them
// (manifestreader.FileKeyPath), and a key can never leave the root: it must be a clean relative slash path, and
// the root is opened with [os.Root], which refuses ".." and symbolic links that point outside.
//
// Every write goes to a file of a unique name in the directory of the key (same file system), is flushed with
// fsync, and is renamed over the final name; the directory is flushed after the rename. A reader sees the old file
// or the new one, never part of one. The names of the files written aside start with ".tmp-"; the reader opens
// only the keys the manifest names and never lists, so the leftover of a killed run is never read and is safe to
// delete.
//
// The conditional write ("create only" or "only if it still has the ETag read") is a read-compare-rename that runs
// under an exclusive advisory lock (flock) on the file ".semaphore-publish.lock" in the root. Publishers that go
// through this package exclude each other, in one process and across processes of one host. The lock does not
// protect against a writer that ignores it, and flock is not reliable on every network file system (NFS before
// version 4 or with locks off): publish from one host to such a store. Writing needs a platform with flock
// (Linux, macOS, the BSDs) or LockFileEx (Windows); elsewhere a write fails with [ErrUnsupported]. Reading (verify)
// works everywhere.
package filepublish
