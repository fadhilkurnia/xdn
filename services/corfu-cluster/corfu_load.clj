; Timed load loop for bandwidth measurement, run through ShellMain like the
; stock corfu_scripts:
;   java -cp "/usr/share/corfu/lib/*" org.corfudb.shell.ShellMain \
;     run-script corfu_load.clj -c replica-0:9000,replica-1:9000,replica-2:9000 \
;     -i bw -d 55000 write
; Run it from a NON-member container attached to the service overlay so the
; client-driven chain write path (token from the sequencer, then a client
; write to each log unit in chain order) is classified as client traffic.
(in-ns 'org.corfudb.shell)
(import org.docopt.Docopt)
(def usage "corfu_load, timed load loop for bandwidth measurement.
Usage:
  corfu_load -c <config> -i <stream-id> -d <ms> [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-g -o <username_file> -j <password_file>]] (write|read)
Options:
  -c <config>, --config <config>                                                         Configuration string to use.
  -i <stream-id>, --stream-id <stream-id>                                                ID or name of the stream to work with.
  -d <ms>, --duration <ms>                                                               Loop duration in milliseconds.
  -e, --enable-tls                                                                       Enable TLS.
  -u <keystore>, --keystore=<keystore>                                                   Path to the key store.
  -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.
  -r <truststore>, --truststore=<truststore>                                             Path to the trust store.
  -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.
  -g, --enable-sasl-plain-text-auth                                                      Enable SASL Plain Text Authentication.
  -o <username_file>, --sasl-plain-text-username-file=<username_file>                    Path to the file containing the username for SASL Plain Text Authentication.
  -j <password_file>, --sasl-plain-text-password-file=<password_file>                    Path to the file containing the password for SASL Plain Text Authentication.
  -h, --help     Show this screen.
")
(def localcmd (.. (new Docopt usage) (parse *args)))

(get-runtime (.. localcmd (get "--config")) localcmd)
(connect-runtime)

(def deadline (+ (System/currentTimeMillis)
                 (Long/parseLong (.. localcmd (get "--duration")))))
(def payload (byte-array 256))
(def sid (uuid-from-string (.. localcmd (get "--stream-id"))))

(if (.. localcmd (get "write"))
  ; write: append one 256B entry per iteration (~10 ops/s)
  (loop [n 0]
    (if (< (System/currentTimeMillis) deadline)
      (do (.. (get-stream sid) (append payload))
          (Thread/sleep 100)
          (recur (inc n)))
      (println (str "OPS=" n))))
  ; read: re-read the whole stream each second (fresh view -> full replay)
  (loop [n 0]
    (if (< (System/currentTimeMillis) deadline)
      (do (.. (get-stream sid) (streamUpTo Long/MAX_VALUE) (toArray))
          (Thread/sleep 1000)
          (recur (inc n)))
      (println (str "OPS=" n)))))
