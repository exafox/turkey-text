import sublime,sublime_plugin
import os,sys,subprocess,threading
from datetime import datetime

TURKEY_DSN = 'Turkey'

def sql_proc():
    return subprocess.Popen(
                        ['isql',TURKEY_DSN,'-bc'],
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                        )


class SqlCall(threading.Thread):

    def __init__(self, sql_statements):
        self.sql_statements = sql_statements
        threading.Thread.__init__(self)

    def run(self):       
        self.data = list() 
        for statement in self.sql_statements:
            proc = sql_proc()
            tstamp = datetime.now()
            out,err = proc.communicate(statement)
            self.data.append((statement,tstamp,out,err))
        self.sql_statements = None



class IsqlCommand(sublime_plugin.TextCommand):
    def log(self,data):
        if type(data) not in (str,unicode):
            data = str(data)
        print data,
        self.log_view.insert(self.edit,self.offset,data)
        self.offset += len(data)

    def run(self, edit):
        """
            Runs selected code in isql using the named DSN.
        """
        #setup
        window = self.view.window()
        self.log_view = window.new_file()
        self.edit = edit
        self.offset = 0 

        #clean code two ways
        sql_statements = '\n'.join([self.view.substr(region) for region in self.view.sel()])
        sql_statements = [x + ';' for x in sql_statements.split(';') if x.strip() != '']

        #immediate feedback confirms somethings happening
        self.log("[%s - %s]\n\n" % (TURKEY_DSN,datetime.now().time().strftime('%H:%M')))
        self.log(' > ' + ''.join(sql_statements).replace('\n','\n > ') + '\n\n')

        #run some things
        caller = SqlCall(sql_statements)
        caller.start()
        threads = [caller]
        self.log_view.end_edit(self.edit)
        self.thread_handler(threads)

    def thread_handler(self,threads):
        not_finished = list()
        for thread in threads:
            self.log('.')
            if not thread.is_alive():
                for statement,tstamp,out,err in thread.data:
                    self.edit = self.log_view.begin_edit('ok')
                    self.log('\n\n\n%s:\n\n%s' % (tstamp.isoformat(),statement.strip()))
                    self.log(err.strip() + '\n\n')
                    self.log(out.strip() + '\n--\n')
                    self.log_view.end_edit(self.edit)
            else:
                not_finished.append(thread)
            if len(not_finished) > 0:
                sublime.set_timeout(lambda: self.thread_handler(not_finished),500)
            else:
                self.log('\n[done]')



