#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys, os, re
import subprocess, signal
import time, optparse

comment_pattern = re.compile("^[;#] *(.*)$")
send_pattern = re.compile("^(?:[>]|send) *(.*)$")
expect_pattern = re.compile("^(?:[=]|expect) *(.*)$")
match_pattern = re.compile("^(?:[~]|match) *(.*)$")
skip_pattern = re.compile("^(?:[*]|skip) *(.*)$")
code_pattern = re.compile("^(?:[?]|returns) *(.*)$")
signal_pattern = re.compile("^(?:[!]|sig|signal) *(.*)$")
close_pattern = re.compile("^(?:[.]|eof|close) *(.*)$")

sigterm_delay = 1 # wait 1 sec for process to terminate


class TalkbackException(Exception):
    def __init__(self, message, line = None):
      
        Exception.__init__(self, message, line)
        self.line = line
        self.message = message
        
    def __str__(self):
        msg = self.message
        
        if self.line is not None:
	  msg = "on line %d: %s" % ( self.line, msg )
	  
	return msg

class ExpectationFailed(TalkbackException):
    def __init__(self, message, line = None):
        TalkbackException.__init__(self, message, line)

class ScriptException(TalkbackException):
    def __init__(self, message, line = None):
        TalkbackException.__init__(self, message, line)


class Talkback:
   def __init__( self, proc, linesep = None, quiet = False ):
      self.proc = proc
      
      if linesep is None:
	linesep = os.linesep
	
      self.linesep = linesep
      self.quiet = quiet

      self.from_child = proc.stdout
      self.to_child = proc.stdin

   def echo(self, s):
      if not self.quiet:
	print s
      
   def send(self, s, line = None):
      try:
	self.to_child.write(s + self.linesep)
	      
      except ValueError:
	 if self.proc.poll() is None:
	    raise ExpectationFailed( "client no longer accepts input! client process still running with pid %d" % self.proc.pid, line )
	 else:
	    raise ExpectationFailed( "client no longer accepts input, client process terminated unexpectedly with code %d" % self.proc.returncode, line )
      
      self.echo( "> " + s )
            
   def recv(self, line = None):
      try:
	s = self.from_child.readline()
      except ValueError, e:
	raise IOError( "can't read from client's stdout pipe: %s" % e )
      
      if s is None or s == "":
	 if self.proc.poll() is None:
	    raise ExpectationFailed( "unexpected EOF! client process still running with pid %d" % self.proc.pid, line )
	 else:
	    raise ExpectationFailed( "unexpected EOF, client process terminated unexpectedly with code %d" % self.proc.returncode, line )
       
      s = s.strip()

      self.echo( "< " + s )
      
      return s

   def expect(self, s, skip = False, line = None):
      while True:
         t = self.recv(line = line)

         if t == s:
            return True         
         
         if not skip:
            raise ExpectationFailed( "expected `%s`, found `%s`" % (s, t), line )
      
   def match(self, s, skip = False, line = None):
      p = re.compile(s)      
      
      while True:
         t = self.recv(line = line)

         if p.search(t):
            return True         
         
         if not skip:
            raise ExpectationFailed( "expected match on pattern `%s`, found `%s`" % (s, t), line )      

   def terminate(self, code = None, skip = False, line = None):
	if self.proc.poll() is None and skip:
	  
	  while True:
	    t = self.from_child.readline() #XXX: this never times out!
	    if t == "" or t is None or t == False:
	      break
	      
	    self.echo( "< " + t.strip() )

	if not skip:
	    t = self.from_child.readline() #assert that there is no input left
	    if t != "" and t is not None and t != False:
	      raise ExpectationFailed( "EOF expected, found `%s`" % t.strip(), line )
	  
	if self.proc.poll() is None:
	  time.sleep( 0.1 ) 
	  
	if self.proc.poll() is None:
	  time.sleep( sigterm_delay ) 
	  
	  if self.proc.poll() is None:
	    raise ExpectationFailed( "client process didn't terminate when expected! sending SIGHUP.", line )
		    
	if self.proc.returncode is not None:
	  if code is not None and self.proc.returncode != code:
	    raise ExpectationFailed( "expected return code %d, found %d" % (code, self.proc.returncode), line )      
	  
	self.echo( "returns %d" % code )

   def signal(self, sig, line = None):
	sig = sig.lowercase()
	
	if sig == "hup" or sig == "sighup":
	  self.child.send_signal(signal.SIGHUP)
      
	elif sig == "term" or sig == "sigterm":
	  self.child.terminate()
      
	elif sig == "kill" or sig == "sigkill":
	  self.child.terminate()
	  
	else:
	  try:
	      sig = int( sig )
	      self.send_signal( sig )
	      
	  except ValueError:
	      raise ScriptException("bad signal: %s" % sig, line )

	self.echo( "signal %s" % sig )
      
   def run(self, script):
      skip = False      
      line = 0
      
      while True:
         s = script.readline()
         line = line + 1
      
         if s is None or s == "":
            break
   
         s = s.strip()
         if s == "":
	   self.echo("")
	   continue
   
         if comment_pattern.match(s): 
	    self.echo(s)
            continue
	  
         m = code_pattern.match(s)
         if m: 
	    try:
		code = int( m.group(1) )
		self.terminate( code, skip, line )
		continue
		
	    except ValueError:
		raise ScriptException("bad return code expectation: %s" % m.group(1), line )
            
         if close_pattern.match(s): 
	    self.to_child.close()
	    self.echo( "close" )
            continue
	  
         m = signal_pattern.match(s)
         if m: 
	    sig = m.group(1)
	    
	    self.signal( sig, line )
	    continue

         m = skip_pattern.match(s)
         if m:
            skip = True
            continue               
	  
	 if self.proc.poll() is not None:
	   raise ExpectationFailed("client process terminated unexpectedly with code %d!" % self.proc.returncode, line )

	 m = send_pattern.match(s)
         if m:
            self.send( m.group(1), line )
            #skip = False
            continue               

         m = expect_pattern.match(s)
         if m:
            self.expect( m.group(1), skip, line )
            skip = False
            continue               

         m = match_pattern.match(s)
         if m:
            self.match( m.group(1), skip, line )
            skip = False
            continue               

         raise ScriptException("unknown instruction: %s" % s, line )
     

if __name__=="__main__":
   parser = optparse.OptionParser( usage= "USAGE: talkpack [options...] <script> <command> [parameters...]" )
   parser.disable_interspersed_args()

   parser.add_option("-q", "--quiet", dest="quiet", action = "store_true",
		      help="quiet mode, turn off echo of conversation", )

   (options, args) = parser.parse_args()

   if len(args) < 2:
     parser.print_usage()
     sys.exit(2)
  
   scriptFile= args[0]
   cmd = args[1:]
   
   if scriptFile == "-":
       script = sys.stdin
   else:
       script = file(scriptFile)
   
   proc = subprocess.Popen( cmd, 
			    bufsize = 1, 
			    universal_newlines = True,
			    stdin = subprocess.PIPE, 
			    stdout = subprocess.PIPE, 
			    stderr = subprocess.STDOUT )

   t = Talkback( proc, quiet = options.quiet )      
   code = 0
   
   try:
      t.run( script )
      print "[OK]"
   except ExpectationFailed, e:
      print "[FAILED]", e
      code = 1
   except ScriptException, e:
      print "[ERROR]", e
      code = 3
   except IOError, e:
      print "[IOERROR]", e
      code = 4
   
   script.close()
   
   if proc.poll() is None:
     time.sleep(0.1)
     
   if proc.poll() is None:
     print "  terminating child process with pid %d" % proc.pid
     proc.terminate()

   if proc.poll() is None:
     time.sleep(sigterm_delay)

   if proc.poll() is None:
     print "  terminating child process with pid %d did not terminate, sending SIGKILL" % proc.pid
     proc.kill()

   if proc.poll() is None:
     time.sleep(sigterm_delay)

   if proc.poll() is None:
     print "  terminating child process with pid %d still did not terminate!" % proc.pid

   sys.exit(code)
