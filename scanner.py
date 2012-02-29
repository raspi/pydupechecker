#!/bin/env/python
# -*- coding: utf8 -*-
# 
# Get list of duplicate files as Python pickle file for later processing
#
# Idea:
# 1. get list of files indexed by file size
# 2. process through that list and calculate hashes of beginning of same sized files (fast)
# 3. process through that list and calculate full checksum of whole files of same sized files
# 4. output pickle file for your next processing
#
# License: BSD
# (c) Pekka Järvinen 2012
#
# TODO:
# - refactoring (TM) :)

import os
import sys
import logging
import hashlib
import pickle

from ConfigParser import ConfigParser
from optparse import OptionParser, Option, OptionGroup

__AUTHOR__ = u"Pekka Järvinen"
__YEAR__ = "2012"
__VERSION__ = "0.0.1"

logger = logging.getLogger()
FORMAT = "%(asctime)s %(levelname)s: %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG, datefmt="%H:%M:%S")


class DirWalker:

  # structure:
  # {
  #   size of file: [path to files with same size]
  #   1234:         ['/foo/bar.jpg', '/quux.jpg'],
  #   1337:         ['/xyzzy.txt']
  # }
  fileListing = {}

  def walk(self, dir):
    logging.getLogger()

    dir = os.path.abspath(dir)

    logging.info("Scanning directory: '%s'.." % dir)

    if not os.path.isdir(dir):
      err = "'%s' is not directory." % dir
      logging.critical(err)
      raise ValueError(err)

    try:
      files = os.listdir(dir)
    except Exception, e: 
      logging.warning("Skipped directory '%s' with message:" % (dir))
      logging.warning(str(e))
      files = []

    for file in files:
      
      filePath = os.path.join(dir, file)
      logging.debug("  File: '%s'" % filePath)

      if os.path.isdir(filePath):
        self.walk(filePath)
        continue
     
      if not os.path.isfile(filePath):
        logging.info("Ignored '%s' (not file)" % (filePath))
        continue

      fileSize = os.path.getsize(filePath)

      try:
        # use file size as index
        self.fileListing[fileSize].append(filePath)
      except:
        self.fileListing[fileSize] = [filePath]

  def getFileListing(self):
    return self.fileListing

# drop all keys without duplicate file sizes from fileListing
def GetDuplicateSizeFiles(fileListing):
  logging.getLogger()  

  # structure:
  # {
  #   size of file: [path to files with same size]
  #   1234:         ['/foo/bar', '/quux'],
  #   1337:         ['/xyzzy.txt']
  # }
  parsed = {}

  numberOfFileSizes = len(fileListing)

  logging.info("Got file listing with %s different filesizes" % (numberOfFileSizes))

  for fileSize in fileListing.keys():

    numberOfPaths = len(fileListing[fileSize])

    if numberOfPaths > 1:
      logging.info("File size of %s contains %s paths" % (fileSize, numberOfPaths))
      parsed[fileSize] = fileListing[fileSize]
    else:
      logging.debug("File size of %s contains only one path, ignoring" % (fileSize))

  logging.info("Original count of file sizes was %s. Found %s with more than one same sizes." % (numberOfFileSizes, len(parsed)))

  return parsed


# Quick hashing
def GetChecksumsOfFilesContent(fileListing):
  # Make quick comparison of start of file content
  logging.getLogger()

  # structure
  # {
  #   file size:      hash: list of file paths 
  #   1234:      {deadbabe: ['/foo/bar', '/quux']}
  # }
  hashes = {}
  
  numberOfFileSizes = len(fileListing)
  logging.info("Got file listing with %s different filesizes" % (numberOfFileSizes))
  
  for fileSize in fileListing.keys():

    sameSizedFiles = fileListing[fileSize]

    # same sized files hashes
    sameSizedHashes = {}

    # final hashes with only more than one file path
    hashesToAdd = {}

    for filePath in sameSizedFiles:
      
      try:
        logging.warning("  Quick hashing beginning of file: '%s'" % (filePath))
        hash = GetFileBeginningHash(filePath)
      except Exception, e:
        logging.warning("Got error while hashing file '%s' with message:" % (filePath))
        logging.warning(str(e))
        continue

      try:
        sameSizedHashes[hash].append(filePath)
      except:
        sameSizedHashes[hash] = [filePath]

    # iterate through hashes and add only those with more than one file path with same hash
    # because those are duplicate candidates
    for hash in sameSizedHashes.keys():
      if len(sameSizedHashes[hash]) > 1:
        logging.debug("Added hash %s to be added to duplicate file path collection" % (hash))
        hashesToAdd[hash] = sameSizedHashes[hash]
      else:
        logging.debug("Only one path with hash %s so it was ignored" % (hash))

    if len(hashesToAdd):
      hashes[fileSize] = hashesToAdd

  return hashes 

# heavy and final calculation
def GetDuplicates(filePathsWithHashes):
  logging.getLogger()

  # structure
  # {
  #   file size:      hash: list of file paths 
  #   1234:      {deadbabe: ['/foo/bar', '/quux']}
  # }
  duplicates = {}

  for fileSize in filePathsWithHashes.keys():

    # same sized
    sameSizedFiles = filePathsWithHashes[fileSize]

    for hash in sameSizedFiles:
      # same hash
      filesWithSameHash = sameSizedFiles[hash]

      # same file sized hashes
      hashes = {}

      # filtered
      filesToAdd = {}

      for file in filesWithSameHash:
        try:
          # full file hash
          logging.info("  Hashing file '%s'.." % (file))
          fileHash = GetFileHash(file)
        except Exception, e:
          logging.warning("Got error while hashing file '%s' with message:" % (file))
          logging.warning(str(e))
          continue

        try:
          hashes[fileHash].append(file)
        except Exception, e:
          hashes[fileHash] = [file]

      for hash in hashes.keys():
        if len(hashes[hash]) > 1:
          filesToAdd[hash] = hashes[hash]
          logging.info("Added hash %s to collection" % (hash))
        else:
          logging.warning("Only one path with hash %s so it was ignored" % (hash))

      if len(filesToAdd):
        duplicates[fileSize] = filesToAdd

  return duplicates


# Full file hashing
def GetFileHash(filePath):
  m = hashlib.sha512()

  with open(filePath, 'rb') as f:

    while True:
      data = f.read(1024)

      if not data:
        break

      m.update(data)

    return m.hexdigest()

  raise IOError("Couldn't parse file %s" % filePath)

# Calculate hash of beginning of file
def GetFileBeginningHash(filePath):
  m = hashlib.sha512()
  with open(filePath, 'rb') as f:
    data = f.read(1024)
    m.update(data)
    return m.hexdigest()
  raise IOError("Couldn't parse file %s" % filePath)



if __name__ == "__main__":
  banner  = u" %s" % (__VERSION__)
  banner += u" (c) %s %s" % (__AUTHOR__, __YEAR__)

  examples = []
  examples.append("--dir /home --file /home.pickle")

  usage = "\n".join(examples)

  parser = OptionParser(version="%prog " + __VERSION__, usage=usage, description=banner)

  parser.add_option("--dir", "-d", action="store", type="string", dest="directory", help="Directory to scan recursively")
  parser.add_option("--file", "-f", action="store", type="string", dest="picklefile", help="Python pickle file", default="duplicates.pickle")

  (options, args) = parser.parse_args()

  scandir = None
  picklefile = None

  if options.directory != "":
    scandir = options.directory

  if scandir is None:
    print "See --help for usage information."
    sys.exit(1)

  dw = DirWalker()
  dw.walk(scandir)

  duplicateFiles = {}

  # get file sizes with paths
  fileListing = dw.getFileListing()

  if len(fileListing):
    # list contains now same sized files
    fileListing = GetDuplicateSizeFiles(fileListing)

    # do a quick hashing of beginning of same sized files 
    # so that we don't need to scan so much data in final hashing
    # ie. drop same sized files with different hashes
    filesWithHashes = GetChecksumsOfFilesContent(fileListing)

    if len(filesWithHashes):
      # final result
      duplicateFiles = GetDuplicates(filesWithHashes)

  if len(duplicateFiles):
    logging.info("Writing Python pickle file containing duplicate files '%s'.." % options.picklefile)
    with open(options.picklefile, 'wb+') as f:
      pickle.dump(duplicateFiles, f)
      logging.info("Pickle file '%s' created." % options.picklefile)
  else:
    logging.info("No duplicate files found.")

  logging.info("Done.")
