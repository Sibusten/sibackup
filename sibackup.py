import argparse
import logging
import os
import shutil
import time
import stat
from profilehooks import profile

"""
Create some kind of "percentage" calculation to give the user an idea of how close it is to being complete. Will be very
complex and requre guessing due to not being able to know ahead of time how many files/folders/data will need to be
processed
"""


class StatHelper:
    """
    A helper class that provides straightforward methods for getting various information from os.stat() results
    Provides better performance than calling the various os.path.* methods multiple times on the same file, due to only
    having to call os.stat() once.
    """
    def __init__(self, path_name):
        self.path_name = path_name
        try:
            # Try to get the stats
            self.stats = os.stat(path_name)
            self._exists = True
            self._has_permission = True
        except FileNotFoundError:
            # The file does not exist
            self._exists = False
            self._has_permission = True
        except PermissionError:
            # The program does not have read access to the file or directory
            self._has_permission = False

    def has_permission(self):
        return self._has_permission

    def exists(self):
        """Return whether the file exists"""
        return self._exists

    def isfile(self):
        """Return True if the path name refers to a regular file"""
        return stat.S_ISREG(self.stats.st_mode)

    def isdir(self):
        """Return True if the path name referes to a directory"""
        return stat.S_ISDIR(self.stats.st_mode)

    def getsize(self):
        """Return the size of the file"""
        return self.stats.st_size

    def getmtime(self):
        """Return the last modification time of the file"""
        return self.stats.st_mtime

    def getatime(self):
        """Return the last access time of the file"""
        return self.stats.st_atime

    def getctime(self):
        """Return the metadata change time (UNIX) or the creation time (Windows) of the file"""
        return self.stats.st_ctime

    def samestat(self, other):
        """Test whether two stat helpers reference the same file"""
        assert isinstance(other, StatHelper)
        return (self.stats.st_ino == other.stats.st_ino and
                self.stats.st_dev == other.stats.st_dev)

    def haswrite(self):
        """Whether the file has write access"""
        return self.stats.st_mode & stat.S_IWRITE

    def getmode(self):
        return self.stats.st_mode


class Timer:
    @classmethod
    def format_time(cls, elapsed_time):
        temp = int(elapsed_time * 1000)
        millis = temp % 1000
        temp //= 1000
        seconds = temp % 60
        temp //= 60
        minutes = temp % 60
        temp //= 60
        hours = temp
        return "{}:{:0>2d}:{:0>2d}.{:0>4d}".format(hours, minutes, seconds, millis)

    def __init__(self):
        self.start_time = 0
        self.last_lap = 0

    def start(self):
        self.start_time = time.perf_counter()
        self.last_lap = self.start_time

    def lap(self):
        last_lap_time = self.last_lap
        self.last_lap = time.perf_counter()
        return self.last_lap - last_lap_time

    def elapsed(self):
        return time.perf_counter() - self.start_time


LOG_FORMAT = "%(asctime)s:%(name)s:%(levelname)s: %(message)s"
LOG_LEVELS = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR,
              'CRITICAL': logging.CRITICAL}

# Average file size copied
# What determined whether each file should be copied
info_data = {
    'files': {
        'num_processed': 0,
        'num_copied': 0,
        'size_copied': 0,
        'num_skipped': 0,
        'size_skipped': 0,
        'num_deleted': 0,
        'not_copied': 0,
    },
    'folders': {
        'num_created': 0,
        'num_deleted': 0,
    },
    'time_spent': {
        'copying': 0,
        'resolving': 0,
        'scanning': 0,
        'stats': 0,
        'hashing': 0,
    },
    'misc': {
        'conflicts_resolved': 0,
        'hashes_made': 0,
        'data_hashed': 0,
    },
    'errors': {

    },
}


def format_data_size(data_size):
    data_size_values = [1, 1024, 1048576, 1073741824, 1099511627776]
    data_size_names = ['B', 'KB', 'MB', 'GB', 'TB']

    # Special case: if the size is empty, return 0B
    if data_size == 0:
        return "0B"

    # Check each tier in reverse: If the data size is greater than one unit of that value, it will be used as the base
    for i in range(len(data_size_values) - 1, 0, -1):
        data_size_value = data_size_values[i]
        if data_size >= data_size_value:
            return "{:.3f}{}".format(
                data_size / data_size_value,
                data_size_names[i],
            )


def sim_text():
    """Return a string to signify that nothing has actually happened if set to simulate, otherwise return nothing"""
    return "<Simulate>: " if args.simulate else ""


def conflict_text(present_tense):
    """Return a string to represent what type of conflict resolution was used"""
    if args.conflictmode == 0:
        return "ignoring" if present_tense else "ignored"
    elif args.conflictmode == 1:
        return "archiving" if present_tense else "archived"
    else:  # args.conflictmode == 2
        return "deleting" if present_tense else "deleted"


def copy_file(source_file_stats, dest_file_stats):
    """Copies a file from the source folder to the destination folder"""
    logger.debug("{}Copying '{}'".format(sim_text(), source_file_stats.path_name))
    if not args.simulate:
        try:
            timer.lap()
            shutil.copy2(source_file_stats.path_name, dest_file_stats.path_name)
            info_data['time_spent']['copying'] += timer.lap()
            info_data['files']['num_copied'] += 1
            info_data['files']['size_copied'] += source_file_stats.getsize()
        except PermissionError:
            logger.warning("Cannot copy file here, access denied: '{}'".format(dest_file_stats.path_name))
            info_data['files']['not_copied'] += 1
        except FileNotFoundError:
            # If this error happens here, it's likely meaning that the destination file could not be found.
            # This is likely due to the destination file path being too long for the OS to handle.
            logger.warning("Cannot copy file here, destination path too long: '{}'".format(dest_file_stats.path_name))
            info_data['files']['not_copied'] += 1
    else:  # Simulate only
        info_data['files']['num_copied'] += 1
        info_data['files']['size_copied'] += source_file_stats.getsize()


def copy_folder(source_path, dest_path, archive_path, current_folder, depth):
    """
    Processes the contents of a source folder and copies them to the destination folder if aplicable.
    This method is called recursively for each subfolder found.
    """
    # If the current depth is too deep, skip the folder
    if args.depth is not None and depth > args.depth:
        logger.debug("Subfolder depth too deep, skipping")
        return 0

    # Create absolute paths for the source, destination, and archive using the current relative folder
    current_source_path = os.path.join(source_path, current_folder)
    current_dest_path = os.path.join(dest_path, current_folder)
    current_archive_path = os.path.join(archive_path, current_folder)

    # Create destination folder if it doesn't exist
    if not os.path.exists(current_dest_path):
        logger.debug("{}Creating destination folder: '{}'".format(sim_text(), current_dest_path))
        if not args.simulate:
            try:
                os.mkdir(current_dest_path)
                info_data['folders']['num_created'] += 1
            except PermissionError:
                logger.warning("Cannot create folder in destination, access denied: '{}'".format(current_dest_path))
                logger.warning("Trying to continue...")
                return 0

    # Get a list of all files and folders in the current source and destination folders
    try:
        timer.lap()
        source_list = os.listdir(current_source_path)
        info_data['time_spent']['scanning'] += timer.lap()
    except PermissionError:  # If the source folder cannot be accessed, skip it and move on
        logger.warning("Cannot read contents of source folder, access is denied: '{}'".format(current_source_path))
        return 0
    try:
        timer.lap()
        dest_list = os.listdir(current_dest_path)
        info_data['time_spent']['scanning'] += timer.lap()
    # Destination folder could not be found for some reason
    except FileNotFoundError:
        if args.simulate:
            # If set to simulate, assume that the folder would have been created if not simulating, and ignore
            dest_list = []
        else:
            # Something is really wrong. This should never happen
            logger.critical("Destination folder could not be found: '{}'".format(current_dest_path))
            return 1

    # Get a list of all files and folders present in the destination folder that are not in the source folder
    conflict_list = list(set(dest_list) - set(source_list))

    # Process each confliction
    for conflict_item in conflict_list:
        # Get the absolute path of the conflict item
        conflict_item_path = os.path.join(current_dest_path, conflict_item)

        # Get conflict stats
        timer.lap()
        conflict_item_stats = StatHelper(conflict_item_path)
        info_data['time_spent']['stats'] += timer.lap()

        if args.conflictmode == 0:
            # Ignore the conflict
            logger.debug("Ignoring conflict: '{}'".format(conflict_item_path))
            info_data['misc']['conflicts_resolved'] += 1
        else:  # The conflict will need to be moved or deleted
            # Make sure that the conflict has write permissions
            if not conflict_item_stats.haswrite():
                logger.debug("File does not have write access, setting it: '{}'".format(conflict_item_path))
                os.chmod(conflict_item_path, stat.S_IWRITE)

            if args.conflictmode == 1:
                # Make sure the archive folder is not archived
                if not conflict_item_path == archive_path:
                    # Archive the conflict
                    if not os.path.exists(current_archive_path):
                        logger.debug("{}Creating archive directory: '{}'".format(sim_text(), current_archive_path))
                        if not args.simulate:
                            try:
                                os.makedirs(current_archive_path)
                                info_data['folders']['num_created'] += 1
                            except PermissionError:
                                logger.warning("Cannot create folder in archive, access denied: '{}'".format(
                                    current_archive_path))
                                logger.warning("Trying to continue...")
                                return 0
                        else:  # Simulate only
                            info_data['folders']['num_created'] += 1
                    logger.debug("{}Archiving conflict: '{}'".format(sim_text(), conflict_item_path))
                    if not args.simulate:
                        try:
                            timer.lap()
                            shutil.move(conflict_item_path, current_archive_path)
                            info_data['time_spent']['resolving'] += timer.lap()
                            info_data['misc']['conflicts_resolved'] += 1
                        except PermissionError:
                            logger.warning("Cannot archive conflict, access denied: '{}'".format(conflict_item_path))
                            continue
                    else:  # Simulate only
                        info_data['misc']['conflicts_resolved'] += 1
            else:
                # Delete the conflict. Use shutil.rmtree for directories and os.remove for files
                logger.debug("{}Deleting conflict: '{}'".format(sim_text(), conflict_item_path))
                if not args.simulate:
                    try:
                        timer.lap()
                        if os.path.isdir(conflict_item_path):
                            def remove_read_only(action, name, exc):
                                os.chmod(name, stat.S_IWRITE)
                            shutil.rmtree(conflict_item_path, onerror=remove_read_only)
                        else:
                            os.remove(conflict_item_path)
                        info_data['time_spent']['resolving'] += timer.lap()
                        info_data['misc']['conflicts_resolved'] += 1
                    except PermissionError:
                        logger.warning("Cannot delete conflict, access denied: '{}'".format(conflict_item_path))
                        continue
                else:  # Simulate only
                    info_data['misc']['conflicts_resolved'] += 1

    # Process each item in the current source folder
    for source_item in source_list:
        # Get the absolute path of the source item
        source_item_path = os.path.join(current_source_path, source_item)

        # Get source stats
        timer.lap()
        source_item_stats = StatHelper(source_item_path)
        info_data['time_spent']['stats'] += timer.lap()

        # If we do not have read access to the current path, log and skip
        if not source_item_stats.has_permission():
            logger.warning("Access is denied: '{}'".format(source_item_stats.path_name))
            return 0

        if source_item_stats.isdir():
            # If it's a directory, recursively call this method with the source item as the new current folder, and
            # the depth raised by 1.
            logger.debug("Travelling into subfolder: '{}'".format(source_item_path))
            res = copy_folder(source_path, dest_path, archive_path, os.path.join(current_folder, source_item), depth+1)
            # If there's an error, abort
            if res is not 0:
                return res
        else:
            # If it's a file, determine whether it should be copied and do so if applicable.
            # Get the absolute path of the destination item
            dest_item_path = os.path.join(current_dest_path, source_item)

            # Get destination stats
            timer.lap()
            dest_item_stats = StatHelper(dest_item_path)
            info_data['time_spent']['stats'] += timer.lap()

            # If the destination file doesn't exist, or the copy mode is set to always copy without checking
            if not dest_item_stats.exists() or args.copymode == 0:
                copy_file(source_item_stats, dest_item_stats)

            # If copy mode is 1 or higher, check if the source file's modified date is not equal to the destination
            elif args.copymode >= 1 and not source_item_stats.getmtime() == dest_item_stats.getmtime():
                # If the modified date of the destination is newer than the source, something is not right...
                if dest_item_stats.getmtime() > source_item_stats.getmtime():
                    logger.debug("Destination file is newer than source: '{}'".format(dest_item_path))
                    # logger.warning("This should not be the case... There is currently no functionality for dealing "
                    #                "with this, so the file will be skipped.")
                    logger.debug("Copying anyway...")
                # Otherwise the source is newer than the destination, and should be copied
                copy_file(source_item_stats, dest_item_stats)

            # If copy mode is 2 or higher and modified dates are equal, check if there is a difference in the file sizes
            elif args.copymode >= 2 and source_item_stats.getsize() != dest_item_stats.getsize():
                copy_file(source_item_stats, dest_item_stats)

            # If copy mode is 3 and all else is inconclusive, check the file hashes to confirm the files are the same.
            elif args.copymode == 3:
                pass  # TODO: implement hash checking

            # If everything checks out, the files are considered to be the same, and will be skipped
            else:
                logger.debug("Skipping file: '{}'".format(source_item_path))
                info_data['files']['num_skipped'] += 1
                info_data['files']['size_skipped'] += source_item_stats.getsize()
            info_data['files']['num_processed'] += 1

    return 0


# @profile()
def sibackup():
    # Start the timer
    timer.start()

    # Get source folder and make sure it exists
    source_path = os.path.abspath(args.source)
    if not os.path.exists(source_path):
        logger.critical("Source path does not exist. Aborting")
        return 1

    # Get destination folder
    dest_path = os.path.abspath(args.destination)
    if not os.path.exists(dest_path):
        logger.debug("{}Destination path does not exist, creating it...".format(sim_text()))
        if not args.simulate:
            try:
                os.makedirs(dest_path)
                info_data['folders']['num_created'] += 1
            except PermissionError:
                # Would be very bad if this fails here
                logger.critical("Cannot create destination folder, access denied. Aborting")
                return 1
        else:  # Simulate only
            info_data['folders']['num_created'] += 1

    # If depth isn't none, make sure it isn't less than 0
    if args.depth is not None and args.depth < 0:
        logger.critical("Depth cannot be less than 0. Aborting")
        return 1

    # Make sure copymode and conflict mode are valid
    if args.copymode not in [0, 1, 2, 3]:
        logger.critical("Invalid copy mode '{}'. Aborting".format(args.copymode))
        return 1
    if args.conflictmode not in [0, 1, 2]:
        logger.critical("Invalid conflict mode '{}'. Aborting".format(args.copymode))
        return 1

    # Get archive path
    archive_path = os.path.join(dest_path, args.archivepath)
    # Create the archive folder if using archive mode and it doesn't exist
    if args.conflictmode == 1 and not os.path.exists(archive_path):
        logger.debug("{}Archive path does not exist, creating it...".format(sim_text()))
        if not args.simulate:
            try:
                os.makedirs(archive_path)
                info_data['folders']['num_created'] += 1
            except PermissionError:
                logger.critical("Cannot create archive folder, access denied. Aborting")
                return 1
        else:  # Simulate only
            info_data['folders']['num_created'] += 1

    status = copy_folder(source_path, dest_path, archive_path, current_folder='', depth=0)

    if status is not 0:
        logger.critical("Process aborted")
    else:
        logger.info("Process complete")

    # Record how long it took the whole process to finish
    total_time_spent = timer.elapsed()

    # -----------------------
    # - Information logging -
    # -----------------------
    logger.info("-"*16)
    logger.info("Processed {} files".format(info_data['files']['num_processed']))

    total_files_copied_or_skipped = info_data['files']['num_copied'] + info_data['files']['num_skipped']
    total_data_copied_or_skipped = info_data['files']['size_copied'] + info_data['files']['size_skipped']

    # Files copied
    if info_data['files']['num_copied'] is not 0:
        logger.info("{}Copied {} in {} files ({:.2f}% of total data, {:.2f}% of total files, {} average size)".format(
            sim_text(),
            format_data_size(info_data['files']['size_copied']),
            info_data['files']['num_copied'],
            100 * (info_data['files']['size_copied'] / total_data_copied_or_skipped),
            100 * (info_data['files']['num_copied'] / total_files_copied_or_skipped),
            format_data_size(info_data['files']['size_copied'] / info_data['files']['num_copied']),
        ))
    # Files skipped
    if info_data['files']['num_skipped'] is not 0:
        logger.info("{}Skipped {} in {} files ({:.2f}% of total data, {:.2f}% of total files, {} average size)".format(
            sim_text(),
            format_data_size(info_data['files']['size_skipped']),
            info_data['files']['num_skipped'],
            100 * (info_data['files']['size_skipped'] / total_data_copied_or_skipped),
            100 * (info_data['files']['num_skipped'] / total_files_copied_or_skipped),
            format_data_size(info_data['files']['size_skipped'] / info_data['files']['num_skipped']),
        ))
    # Files not copied due to errors
    if info_data['files']['not_copied'] is not 0:
        logger.info("{}{} files not copied due to errors".format(
            sim_text(),
            info_data['files']['not_copied'],
        ))

    # Folders created
    if info_data['folders']['num_created'] is not 0:
        logger.info("{}Created {} folders".format(
            sim_text(),
            info_data['folders']['num_created'],
        ))

    if info_data['misc']['conflicts_resolved'] is not 0:
        logger.info("{}{} conflicts {}".format(
            sim_text(),
            info_data['misc']['conflicts_resolved'],
            conflict_text(present_tense=False),
        ))

    # Total time spent
    logger.info("Finished in {}".format(Timer.format_time(total_time_spent)))
    # Time spent copying
    if info_data['time_spent']['copying'] is not 0:
        logger.info("Spent {} copying files ({:.2f}% of total time)".format(
            Timer.format_time(info_data['time_spent']['copying']),
            100 * (info_data['time_spent']['copying'] / total_time_spent)
        ))
    # Time spent resolving conflicts
    if info_data['time_spent']['resolving'] is not 0:
        logger.info("Spent {} {} files ({:.2f}% of total time)".format(
            Timer.format_time(info_data['time_spent']['resolving']),
            conflict_text(present_tense=True),
            100 * (info_data['time_spent']['resolving'] / total_time_spent)
        ))
    # Time spent scanning directories
    if info_data['time_spent']['scanning'] is not 0:
        logger.info("Spent {} scanning directories ({:.2f}% of total time)".format(
            Timer.format_time(info_data['time_spent']['scanning']),
            100 * (info_data['time_spent']['scanning'] / total_time_spent)
        ))
    # Time spent checking file stats
    if info_data['time_spent']['stats'] is not 0:
        logger.info("Spent {} checking file stats ({:.2f}% of total time)".format(
            Timer.format_time(info_data['time_spent']['stats']),
            100 * (info_data['time_spent']['stats'] / total_time_spent)
        ))
    # Time spent hashing
    if info_data['time_spent']['hashing'] is not 0:
        logger.info("Spent {} hashing files ({:.2f}% of total time)".format(
            Timer.format_time(info_data['time_spent']['hashing']),
            100 * (info_data['time_spent']['hashing'] / total_time_spent)
        ))

    logger.info("-"*16)

    return status


if __name__ == '__main__':
    # Setup the arguments used by the program
    parser = argparse.ArgumentParser(description="Description")
    # Positional
    parser.add_argument('source', type=str,
                        help="Source folder where files will be copied from")
    parser.add_argument('destination', type=str,
                        help="Destination folder where files will be copied to")
    # Optional
    parser.add_argument('-d', '--depth', type=int, default=None,
                        help="Limit the number of subfolders to copy. '0' will copy only files in the source folder, "
                             "'1' will copy only 1 level of subfolders, etc.")
    parser.add_argument('-m', '--copymode', type=int, default=2,
                        help="How to check if a file should be copied. Checks are done in order, and break when one "
                             "succeeds. '3' will check modified date first, then file size if dates are the same, "
                             "then file hashes if sizes are the same. [0: Always copy, 1: Check modified date, "
                             "2: Check file size, 3: Compare file hash (NOT IMPLEMENTED)]")
    parser.add_argument('-M', '--conflictmode', type=int, default=2,
                        help="How to handle conflicts in the destination folder. [0: Do nothing, 1: Move the file to "
                             "the archive folder, 2: Delete the file]")
    parser.add_argument('-a', '--archivepath', type=str, default='!!archive',
                        help="Folder to use for archiving if enabled. Path will be relative to the destination "
                             "folder, but an absolute path will also work.")
    parser.add_argument('-s', '--simulate', action='store_true',
                        help="Simulate copying the folder without actually doing anything. Useful for debugging or "
                             "estimating how much will be copied.")
    # Logging
    logging_group = parser.add_argument_group('logging', 'Options related to the output log')
    logging_group.add_argument('--loglevel', type=str, default="INFO",
                               help="Set logging verbosity. One of (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    logging_group.add_argument('--logfile', type=str, default=None,
                               help="Log file to write to. Will write to console if not provided.")
    logging_group.add_argument('--logfilemode', type=str, default='w',
                               help="File writing mode. 'a' to append onto the existing log, 'w' to overwrite each "
                                    "time the program is run")

    # Parse the arguments provided by the user
    args = parser.parse_args()

    # Set up the root logger
    root_logger = logging.getLogger()

    # Attempt to retrieve the user's desired log level, using the default if an invalid one is provided
    try:
        log_level = LOG_LEVELS[args.loglevel.upper()]
    except KeyError:
        logging.warning("Invalid log level provided ('{}'): Using default log level of INFO".format(args.loglevel))
        log_level = LOG_LEVELS['INFO']
    root_logger.setLevel(log_level)

    # If a log file is set, use a file handler: otherwise use a default stream handler (sys.stderr)
    if args.logfile is not None:
        log_handler = logging.FileHandler(args.logfile, mode=args.logfilemode, encoding='utf8')
    else:
        log_handler = logging.StreamHandler()

    # Set the logging format
    log_formatter = logging.Formatter(LOG_FORMAT)
    log_handler.setFormatter(log_formatter)

    # Apply the handler to the root logger
    root_logger.addHandler(log_handler)

    # logging.basicConfig(**logger_config)

    logger = logging.getLogger(__name__)

    logger.debug("------------------------")
    logger.debug("- Application starting -")
    logger.debug("------------------------")

    timer = Timer()

    try:
        application_status = sibackup()
    except KeyboardInterrupt:
        logger.info("Process cancelled by user, exiting...")
        application_status = 130
    except Exception as e:
        logger.critical("Unexpected error! Please report this to the developers so it can be properly fixed")
        logger.exception(e)
        raise

    logger.debug("Finished with status code {}".format(application_status))
    exit(application_status)
