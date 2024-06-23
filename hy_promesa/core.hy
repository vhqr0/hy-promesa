(eval-when-compile
  (hy.I.hy-functional._bootstrap))

(import
  asyncio
  threading)

;;; macros

(defmacro mlet [bindings #* body]
  (let [mlet-cast (hy.gensym)]
    (defn _mlet [bindings body]
      (let [$ (hy.gensym)]
        (match bindings
          #() body
          #(name form) `(.then (~mlet-cast ~form)
                               (fn [~$]
                                 (let [~name ~$]
                                   ~body)))
          #(#* bindings name form) (_mlet `[~@bindings]
                                     (_mlet `[~name ~form]
                                       body)))))
    `(let [~mlet-cast hy.I.hy-promesa.core.cast]
       ~(_mlet bindings `(do ~@body)))))

;;; executor

(defclass Executor []
  (defn __init__ [self]
    (setv self.tasks (set)
          self.loop (asyncio.new-event-loop)
          self.thread (threading.Thread :target self.run-forever)))

  (defn run-forever [self]
    (asyncio.set-event-loop self.loop)
    (try
      (.run-forever self.loop)
      (finally
        (.run-until-complete self.loop (.shutdown-asyncgens self.loop))
        (.close self.loop))))

  (defn start [self]
    (.start self.thread))

  (defn stop [self]
    (.stop self.loop))

  (defn join [self]
    (.join self.thread))

  (defn submit [self coro]
    (.call-soon-threadsafe self.loop
                           (fn []
                             (let [task (.create-task self.loop coro)]
                               (.add self.tasks task)
                               (.add-done-callback task self.tasks.discard))))))

(def *executor* (Executor))
(def start *executor*.start)
(def stop *executor*.stop)
(def join *executor*.join)

;;; promise

(defclass Promise []
  (defn __init__ [self awaitable [executor *executor*]]
    (setv self.executor executor
          self.event (asyncio.Event)
          self.ret None
          self.exc None)
    (ignore
      (.submit executor
               ((fn :async []
                  (try
                    (setv self.ret (await awaitable))
                    (except [exc #(Exception asyncio.CancelledError)]
                      (setv self.exc exc))
                    (finally
                      (.set self.event))))))))

  (defn [classmethod] wrap-ret [cls ret [executor *executor*]]
    (cls
      ((fn :async [] ret))
      :executor executor))

  (defn [classmethod] wrap-exc [cls exc [executor *executor*]]
    (cls
      ((fn :async [] (raise exc)))
      :executor executor))

  (defn [classmethod] cast [cls obj [executor *executor*]]
    (cond
      (isinstance obj Promise)       obj
      (isinstance obj abc.Awaitable) (cls obj :executor executor)
      (isinstance obj BaseException) (cls.wrap-exc obj :executor executor)
      True                           (cls.wrap-ret obj :executor executor)))

  (defn [property] value [self]
    (if self.exc
      (raise self.exc)
      self.ret))

  (defn :async resolve [self]
    (await (.wait self.event))
    self.value)

  (defn :async resolve* [self]
    (let [self self]
      (while True
        (await (.wait self.event))
        (if (and (none? self.exc) (isinstance self.ret Promise))
          (setv self self.ret)
          (break)))
      self.value))

  (defn handle [self callback]
    (Promise
      ((fn :async []
         (await (.wait self.event))
         (callback self)))
      :executor self.executor))

  (defn handle* [self callback]
    (Promise
      ((fn :async []
         (let [self self]
           (while True
             (await (.wait self.event))
             (if (and (none? self.exc) (isinstance self.ret Promise))
               (setv self self.ret)
               (break)))
           (callback self))))
      :executor self.executor))

  (defn then [self callback]
    (.handle self
             (fn [p]
               (if p.exc
                 (raise p.exc)
                 (callback p.ret)))))

  (defn then* [self callback]
    (.handle* self
              (fn [p]
                (if p.exc
                  (raise p.exc)
                  (callback p.ret)))))

  (defn catch [self callback]
    (.handle self
             (fn [p]
               (if p.exc
                 (callback p.exc)
                 p.ret))))

  (defn catch* [self callback]
    (.handle* self
              (fn [p]
                (if p.exc
                  (callback p.exc)
                  p.ret))))

  (defn finally [self callback]
    (.handle self
             (fn [_]
               (callback))))

  (defn finally* [self callback]
    (.handle* self
              (fn [_]
                (callback)))))

(def cast Promise.cast)

;;; utils

(defn auto-stop [p]
  (.handle* p (fn [p] (.stop p.executor))))

(defn sleep [secs [ret None] [executor *executor*]]
  (Promise (asyncio.sleep secs ret) :executor executor))

(defn wait-for [p timeout [executor *executor*]]
  (Promise
    (asyncio.wait-for (.resolve* p) timeout)
    :executor executor))

(defn gather [#* ps [executor *executor*]]
  (Promise
    (asyncio.gather
      #* (map (fn [p] (.resolve* p)) ps))
    :executor executor))

;;; export

(export
  :objects [Executor *executor* start stop join Promise cast
            auto-stop sleep wait-for gather]
  :macros [mlet])

;;; test

(comment
  (eval-and-compile
    (hy.I.hy-functional._bootstrap)
    (hy.I.hy-promesa._bootstrap)
    (import
      asyncio))
  (.start p.*executor*)
  (def host "www.baidu.com")
  (-> (p.mlet [[reader writer] (asyncio.open-connection host 80)]
        (-> (p.mlet [_ (.write writer (.encode (.format "GET / HTTP/1.1\r\nHost: {}\r\n\r\n" host)))
                     _ (.drain writer)
                     content (.read reader 4096)]
              (.decode content))
            (.then* (fn [content]
                      (print content)))
            (.finally* (fn []
                         (.close writer)))))
      (.catch* (fn [exc]
                 (print "except:" exc)))
      (p.auto-stop))
  (.join p.*executor*)
  )
