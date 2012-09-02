import sublime,sublime_plugin
import os,sys,subprocess,threading
from datetime import datetime
import json,re
from functools import partial
sql_comments = re.compile('\/\*.+?\*\/')

#Example localsettings.json
#{
#    "ROOT":"/path/to/output/folder",
#    "SQL_COMMAND":"sqlcmd -h db.some.domain -U jerkface -w securityftw"],
#    "SQL_EXPORT_COMMAND":"-o {outfile}" //path based off ROOT dir
#}

settings = json.loads(open('localsettings.json').read())
banned_words = ['<%= schema %> .','<%= schema %>.']

def sql_proc(data=None,export=False):
    if export:
        cmd = ' '.join([settings['SQL_COMMAND'],settings['SQL_EXPORT_COMMAND']])
    else:
        cmd = settings['SQL_COMMAND']
    cmd = cmd.format(**data)
    print '[command %s]' % cmd
    cmd = cmd.split()
    return subprocess.Popen(
                        cmd,
                        stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                        )


class SqlCall(threading.Thread):

    def __init__(self, sql_statements, export, slug):
        self.sql_statements = sql_statements
        self.export = export
        self.slug = slug
        threading.Thread.__init__(self)


    def run(self):       
        # settings = json.loads(open('localsettings.json').read())
        self.data = list() 
        for statement in self.sql_statements:

            is_select = statement.strip().startswith('select')

            if not statement.strip().endswith(';'):
                statement += ';'
                statement = statement.lower()
            if is_select and not self.export:
                #limit results if returning them on the log tab
                statement = statement.rstrip(';') + '\nlimit 1000;'
            statement += ' commit;'
            statement = statement
            if self.slug:
                fname = os.path.join(settings['ROOT'],'%s_%s.csv' % (self.slug,datetime.now().time().isoformat()))
            else:
                fname = datetime.now().time().isoformat() + '.csv'
            proc = sql_proc(data=dict(outfile=fname),export=is_select and self.export)
            tstamp = datetime.now()
            out,err = proc.communicate(statement)
            if is_select and self.export:
                out += 'output rendered to %s' % fname
            self.data.append((statement,tstamp,out,err))
        self.sql_statements = None



class SqlCommand(sublime_plugin.TextCommand):
    def log(self,data):
        if type(data) not in (str,unicode):
            data = str(data)
        self.log_view.insert(self.edit,self.offset,data)
        self.offset += len(data)
    def run(self,edit):
        self._run(edit)
    def _run(self, edit, export=False,slug=None):
        """
            Runs selected code in isql using the named DSN.
        """
        #setup
        window = self.view.window()
        self.log_view = window.new_file()
        self.edit = edit
        self.offset = 0 

        #strip comments & clean code two ways
        sql_statements = '\n'.join([self.view.substr(region) for region in self.view.sel()])
        for comment in sql_comments.findall(sql_statements):
            sql_statements = sql_statements.replace(comment,'')
        sql_statements = '\n'.join([line.split('--').pop(0) for line in sql_statements.split('\n')])
        for banned_word in banned_words:
            sql_statements = sql_statements.replace(banned_word,'')
        sql_statements = [x + ';' for x in sql_statements.split(';') if x.strip() != '']

        #immediate feedback confirms somethings happening
        if export:
            self.log("%s-Turkey-%s.log\n\n" % (datetime.now().time().strftime('%H:%M'),slug))
        else:
            self.log("%s-Turkey.log\n\n" % datetime.now().time().strftime('%H:%M'))
        self.log(' > ' + ''.join(sql_statements).replace('\n','\n > ') + '\n\n')

        #run some things
        caller = SqlCall(sql_statements,export,slug)
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
                sublime.set_timeout(lambda: self.thread_handler(not_finished),1000)
            else:
                self.log('\n[done]')
def nothing(val):
    pass
class SqlexportCommand(SqlCommand):
    def run(self,edit):
        self.edit = edit
        self.view.window().show_input_panel('Name this export fool', 'Export', partial(self._run), nothing, nothing)
    def _run(self,val):
        SqlCommand._run(self,self.edit,export=True,slug=val)


