import clamd


def scan_file(clamd_host, file):
    """Scan file content using ClamD.

    param str clamd_host: ClamD host name
    param FileStorage file: File object
    """
    scanner = clamd.ClamdNetworkSocket(host=clamd_host, timeout=30)
    # scanner = clamd.ClamdUnixSocket("/var/run/clamav/clamd.sock")
    # scanner.ping()
    # scanner.version()
    #
    # scanner.scan("/scandir/.gitignore")
    # -> {'/scandir/.gitignore': ('OK', None)}
    # scanner.scan("/scandir/eicar.com.txt")
    # -> {'/scandir/eicar.com.txt': ('FOUND', 'Win.Test.EICAR_HDB-1')}
    #
    try:
        result = scanner.instream(file)
    except IOError:
        # Ping the server if it fails than the server is down
        scanner.ping()
        # Server is up. This means that the file is too big.
        return "File is too large for ClamD to scan"
    finally:
        # rewind file object
        file.seek(0)

    if result and result['stream'][0] == 'FOUND':
        return result['stream'][1]
    return None
