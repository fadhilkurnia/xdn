"""CloudLab geni-lib context helpers.

geni-lib ships a `build-context` CLI, but it is Python-2-only (bare `print`
statements) and crashes on the Python 3 interpreter we run here. This module
reproduces exactly what `build-context --type cloudlab` does, in Py3-safe code:

  * parse the user URN out of the X509 cert (cloudlab.pem),
  * copy the cert + pubkey into ~/.bssw/geni/, and
  * write ~/.bssw/geni/context.json (framework "emulab-ch2"),

which `geni.util.loadContext()` then reads. You only need to build the context
once; afterwards every script just calls `load()`.
"""

import datetime
import glob
import os
import threading
import warnings

# geni-lib's util.py compares cert expiry with the deprecated naive
# `cert.not_valid_after` (should be `not_valid_after_utc`), which makes modern
# `cryptography` emit a CryptographyDeprecationWarning on every loadContext().
# It's a library bug we can't fix at the source (site-packages), and it's
# harmless, so silence just that one warning.
try:
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.filterwarnings(
        "ignore",
        category=CryptographyDeprecationWarning,
        message="Properties that return a na",  # "...naïve datetime..."
    )
except Exception:  # pragma: no cover - never block on a warnings-filter issue
    pass

import geni.util
import geni._coreutil as GCU


def _apply_py3_patches():
    """Work around geni-lib 0.9.9.4 bugs that only bite on Python 3.

    geni-lib imports cleanly on Py3 but its credential path is not Py3-safe:
    ``getUserCredentials`` / ``getSliceCredentials`` return a ``str`` (the
    XML-RPC ``geni_value``), while the cache layer writes it with
    ``open(path, "wb+")`` -- a binary write of a str, which raises "a bytes-like
    object is required, not 'str'" (and, worse, truncates the file to empty
    first, poisoning the cache). Every consumer re-reads the credential *from
    that file* (latin-1), never from the return value, so returning ``bytes``
    here is safe and fixes the write. We encode latin-1 to byte-exactly
    round-trip the later read.
    """
    import geni.aggregate.frameworks as fw

    targets = ("getUserCredentials", "getSliceCredentials")
    for cls_name in ("CHAPI2", "CHAPI1", "Portal", "EmulabCH2", "ProtoGENI"):
        cls = getattr(fw, cls_name, None)
        if cls is None:
            continue
        for meth_name in targets:
            if meth_name not in cls.__dict__:
                continue
            orig = getattr(cls, meth_name)
            if getattr(orig, "_xdn_py3_patched", False):
                continue

            def make(orig):
                def wrapper(self, *args, **kwargs):
                    cred = orig(self, *args, **kwargs)
                    if isinstance(cred, str):
                        cred = cred.encode("latin-1")
                    return cred
                wrapper._xdn_py3_patched = True
                return wrapper

            setattr(cls, meth_name, make(orig))


_scred_lock = threading.Lock()


def _patch_slicecred_path():
    """Make slice-credential access safe for parallel workers.

    geni-lib's ``SliceCredInfo.path`` re-downloads the slice credential whenever
    it expires within *three days* -- i.e. on EVERY access for a short-lived
    (hours) test slice. Each download does ``open(path, "wb+")`` on the SAME
    shared file, so concurrent workers corrupt it. Patch ``path`` to re-download
    only when the credential is *actually* expired, serialized by a lock.
    """
    import geni.aggregate.context as cm
    SCI = getattr(cm, "SliceCredInfo", None)
    if SCI is None or getattr(SCI, "_xdn_path_patched", False):
        return
    orig_download = SCI._downloadCredential

    def path_getter(self):
        with _scred_lock:
            if self.expires is None or self.expires <= datetime.datetime.now():
                orig_download(self)
        return self._path

    SCI.path = property(path_getter)
    SCI._xdn_path_patched = True


_apply_py3_patches()
_patch_slicecred_path()


def _purge_empty_creds():
    """Remove zero-byte cached credential files (user *and* slice creds).

    If a credential fetch dies mid-write (e.g. the Py3 bug above before it was
    patched), ``open(path, "wb+")`` has already truncated the file to empty.
    geni-lib then sees the file *exists* and tries to parse it -> lxml
    "Document is empty". Deleting empties forces a clean re-fetch.
    """
    datadir = GCU.getDefaultDir()
    for pattern in ("*-usercred.xml", "*-scred.xml"):
        for path in glob.glob("%s%s" % (datadir, pattern)):
            try:
                if os.path.getsize(path) == 0:
                    os.remove(path)
            except OSError:
                pass


def context_path():
    """Path geni-lib loads the context from (~/.bssw/geni/context.json)."""
    return "%scontext.json" % (GCU.getDefaultDir(),)


def parse_user_urn(cert_path):
    """Extract the urn:publicid user URN from a CloudLab cert's SubjectAltName.

    This mirrors build-context's parseCert(); geni-lib needs the URN (and the
    username derived from it) to authenticate AM API calls.
    """
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend

    with open(os.path.expanduser(cert_path), "rb") as fh:
        cert = x509.load_pem_x509_certificate(fh.read(), default_backend())
    san = cert.extensions.get_extension_for_oid(
        x509.OID_SUBJECT_ALTERNATIVE_NAME)
    for uri in san.value.get_values_for_type(x509.UniformResourceIdentifier):
        if uri.startswith("urn:publicid"):
            return uri
    raise ValueError(
        "no urn:publicid SubjectAltName found in %s -- is this a CloudLab "
        "'Download Credentials' cert?" % (cert_path,))


def build(cert_path, pubkey_path, project, out_path=None):
    """Create (or overwrite) the geni-lib context from a cloudlab.pem.

    cert_path   : the cloudlab.pem downloaded from the portal (cert + key).
    pubkey_path : your SSH *public* key (.pub); installed on provisioned nodes.
    project     : your CloudLab project name (the default project for slices).
    """
    cert_path = os.path.expanduser(cert_path)
    pubkey_path = os.path.expanduser(pubkey_path)
    if not os.path.exists(cert_path):
        raise FileNotFoundError("cert not found: %s" % (cert_path,))
    if not os.path.exists(pubkey_path):
        raise FileNotFoundError("ssh pubkey not found: %s" % (pubkey_path,))

    user_urn = parse_user_urn(cert_path)
    username = user_urn.split("+")[-1]
    # cloudlab.pem holds both cert and (encrypted) key, so key_path == cert_path.
    geni.util._buildContext(
        "emulab-ch2", cert_path, cert_path, username, user_urn,
        pubkey_path, project, out_path)
    return context_path() if out_path is None else out_path


def load(passphrase=None):
    """Load the saved context.

    The cloudlab.pem private key is passphrase-encrypted (the passphrase you set
    when downloading credentials). Pass the string, or True to be prompted, or
    None for an unencrypted key.
    """
    _purge_empty_creds()
    return geni.util.loadContext(key_passphrase=passphrase)
