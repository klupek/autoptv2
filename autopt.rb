#!/usr/bin/ruby

require 'rubygems'
gem 'activerecord'
require 'active_record'
require 'file-tail'
require 'thread'
require 'yaml'
require 'open-uri'

def irctime_to_time(str)
	str = str.gsub(/^\s*(.+?)\s*$/,'\1')
	if str =~ /^[0-9]+$/
		return Time.parse(str) # moj format
	elsif m = str.match(/^(\d+)\/(\d+)\/(\d+) (\d+):(\d+):(\d+)/)
		return Time.parse( m[3] + m[2] + m[1] + m[4] + m[5] + m[6] )
	elsif m = str.match(/^(\d\d):(\d\d)/)
		return Time.now # bad logs, let it be 'laests'
	end
end

def to_bytes(str)
	str.gsub(/^\s+|\s+$/,'')
	val = str.gsub(/^(\d+).*$/,'').to_f
	unit = str.gsub(/^\d+\s*/)
	if unit == 'TB'
		return val * 1024 * 1024 * 1024 * 1024
	elsif unit == 'GB'
		return val * 1024 * 1024 * 1024
	elsif unit == 'MB'
		return val * 1024 * 1024
	elsif unit =~ /kB/i
		return val * 1024
	else
		return val
	end
		
end


class ConfigFile
	def initialize(fn)
		@fn = fn
		@data = YAML.load(File.open(fn))
	end

	def method_missing(m, *args, &block)
		@data[m]
	end

	def save
		newfn = @fn + ".new"
		File.write(newfn, @data.to_yaml)
		File.rename(newfn, @fn)
	end
end

trap 'INT' do
  Thread.list.each do |thread|
    STDERR.puts "Thread-#{thread.object_id.to_s(36)}"
    STDERR.puts thread.backtrace.join("\n    \\_ ")
  end
  exit
end

config = ConfigFile.new('config.yaml')

def indb(&block) 
	block.call
end

Thread::abort_on_exception=true
ActiveSupport::Deprecation.silenced = true
ActiveRecord::Base.logger = nil
ActiveRecord::Base.configurations = { 'default' => config.database }
ActiveRecord::Base.establish_connection('default')

ActiveRecord::Base.connection.execute("PRAGMA cache_size=102400")

class Release < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE releases(id INTEGER PRIMARY KEY AUTOINCREMENT, source varchar(32) not null, name varchar(255) not null, added TIMESTAMP not null, object text not null)')
	end
	
	def self.from_object(object)
		r = Release.new
		r.name = object[:name]
		r.source = object[:source]
		r.added = object[:time]
		r.object = object.to_yaml
		r
	end
end

class ArchivedRelease < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE archived_releases(id INTEGER PRIMARY KEY AUTOINCREMENT, source varchar(32) not null, name varchar(255) not null, added TIMESTAMP NOT NULL, archived TIMESTAMP not null, object text not null)')
	end

	def self.from_release(release)
		r = ArchivedRelease.new
		r.name = release.name
		r.source = release.source
		r.added = release.added
		r.archived = Time.now
		r.object = release.object
		r
	end

	def self.from_object(object)
		r = ArchivedRelease.new
		r.name = object[:name]
		r.source = object[:source]
		r.added = object[:time]
		r.archived = Time.now
		r.object = object.to_yaml
		r
	end
end

class MissingRelease < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE missing_releases(id INTEGER PRIMARY KEY AUTOINCREMENT, source varchar(32) not null, name varchar(255) not null, log text not null)')
	end
end

class Filter < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE filters(id INTEGER PRIMARY KEY AUTOINCREMENT, name_regex varchar(255))')
	end

	def self.generate_super_regex
		print "Generating uberregex... "
		result = indb { Regexp.new('^((' + Filter.find(:all).map { |x| x.name_regex }.join(')|(') + '))$') }
		puts "ok."
		result
	end
end

class DownloadHistory < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE download_histories(id INTEGER PRIMARY KEY AUTOINCREMENT, name varchar(255), quality varchar(32))')
	end
end

class AdlEntry < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE adl_entries(id INTEGER PRIMARY KEY AUTOINCREMENT, type varchar(10) not null, data varchar(255) not null)')
	end

	AdlEntry.inheritance_column = 'niematakiej'
end

class DeferredDownload < ActiveRecord::Base
	def self.create_table
		ActiveRecord::Base.connection.execute('CREATE TABLE deferred_downloads(id INTEGER PRIMARY KEY AUTOINCREMENT, url text not null, object text not null, log text not null)')
	end

	def self.from_object(object, log)
		DeferredDownload.new(:object => object.to_yaml, :url => object[:url], :log => log) 
	end
end

[ Release, ArchivedRelease, Filter, DownloadHistory, AdlEntry, MissingRelease, DeferredDownload ].each do |klass|
	begin
		klass.create_table
	rescue ActiveRecord::StatementInvalid 
	end
end

ActiveRecord::Base.connection.execute('create unique index if not exists releases_namesource on releases(name,source)')
ActiveRecord::Base.connection.execute('create index if not exists adl_types on adl_entries(type)')
ActiveRecord::Base.connection.execute('create index if not exists download_histories_name on download_histories(name)')
ActiveRecord::Base.connection.execute('create index if not exists mising_releases_name on missing_releases(name)')

exit if ARGV[0] == 'generate-sql-tables-only'

class DeferredDownloader

	def initialize(downloader)
		@downloader = downloader
	end

	def delay(tries)
		if tries == 0
			return 5*60
		elsif tries == 1
			return 30*60
		elsif tries == 2
			return 120*60
		else
			return 24*60*60
		end
	end 
	
	def run
		puts "DEFERRED: Starting"
		dds = indb { DeferredDownload.find(:all).map { |x| x } }
		if dds.empty?	
			puts "DEFERRED: Nothing to do"	
		else
			now = Time.now.to_i
			checked = 0
			dds.each do |dd|
				object = YAML::load(dd.object)
				object[:tries] ||= 0
				object[:lasttry] ||= 0 
				if Time.now.to_i >= object[:lasttry] + delay(object[:tries])
					puts "DEFERRED-TRY: #{object[:name]}"
					if @downloader.download(object, false)
						indb { DeferredDownload.delete_all([ 'id = ?', dd.id]) }
						puts "DEFERRED-OK: #{object[:name]}"
					else 
						if indb { MissingRelease.find(:first, :conditions => [ 'name = ? and source = ?', object[:name], object[:source] ]) }
							indb { DeferredDownload.delete_all([ 'id = ?', dd.id]) }
							puts "DEFERRED-404: #{object[:name]}"
						else
							puts "DEFERRED-DEFER: #{object[:name]} will be retried in #{delay(object[:tries])} seconds"								
							object[:tries] += 1
							object[:lasttry] = Time.now.to_i
							dd.object = object.to_yaml
							indb { dd.save! }
						end
					end
					checked += 1
				end
			end
			puts "DEFERRED-REPORT: #{checked} (re)tried downloads, #{dds.count-checked} waiting"
		end
	end	
end	


class LogWatcher
	def initialize(config,modules, deferred)
		@config = config
		@modules = modules
		@files = config.sources.map { |logfile,src|
			f = File.open(logfile)
#			f.seek(src[:offset] || 0, IO::SEEK_SET)
			{ :file => f, :module => src[:module], :filename => logfile }
		}
		@threads = []
		@consumer_thread = nil
		@ddownloader = deferred
		@queue = Queue.new
	end	

	def start
		@files.each { |f|
			@threads << Thread.new(f) { |fh|
				puts "Reading backlog for " + fh[:filename]
				lines = 0
				ActiveRecord::Base.transaction do
					begin
						loop do 
							line = fh[:file].readline
							@queue.push({ :line => line, :module => fh[:module], :filename => fh[:filename], :offset => fh[:file].pos}) unless line == ''
							lines += 1
						end
						rescue EOFError
					end	
				end
				puts "Read #{lines} lines of backlog in " + fh[:filename]
				fh[:file].extend(File::Tail)
				fh[:file].interval = 10
				fh[:file].backward(10)
				puts "LIVE: Streaming live data from #{fh[:filename]}"
				fh[:file].tail { |line|
					@queue.push({ :line => line, :module => fh[:module], :filename => fh[:filename], :offset => fh[:file].pos})	
				}
			}
		}
		@consumer_thread = Thread.new { 
			lasttask = Time.now.to_i
			lastdd = 0
			dddelay = 6*60
			taskwarn = 120
			loop {
				if Time.now.to_i - lastdd > dddelay
					lastdd = Time.now.to_i
					@ddownloader.run
				end
				if Time.now.to_i - lasttask > taskwarn
					puts "No new tasks in #{taskwarn} seconds"
					lasttask = Time.now.to_i
				end
				begin
					loop {
						task = @queue.pop(true)
						lasttask = Time.now.to_i
						if task == :die
							puts "LogWatcher is shutting down"
							break
						else					
						#	@config.sources[task[:filename]][:offset] = task[:offset]
						#	@config.save # TODO: do it less frequent
							@modules[task[:module]].consume(task[:line])
						end
					}
				rescue ThreadError 
					sleep 1
				end
			}
		}
	end

	def join
		@consumer_thread.join
	end	

	def stop
		@files.each { |fh| fh[:file].close }
		@threads.each { |thread| thread.join }
		@queue << :die
		@consumer_thread.join

		@threads = []
		@consumer_thread = nil
		@files = []
	end
end

#         }.find_all { |x| x }.map { |tf|
#                                 }
#                                         wget = IO.popen("wget -P \"#{ConfigTable.get('torrent-watch-directory')}\" -i -",'w')
#                                                 wget.write(targets.join("\n"))
#                                                         wget.close
#

class PTModule
	def initialize(config, receivers)
		@config = config
		@receivers = receivers
	end
	def consume(line)
		if line =~ /::: PolishTracker :::\s+Torrent.+details\.php/
			m = line.match(/details\.php\?id=(\d+)/)
			return unless m
			id = m[1].to_i	
                                        
			m = line.match(/Torrent\s+\(\s+(.+?)\s+\)/)
			return unless m
			name = m[1]
			
			m = line.match(/Kategoria\s*:\s+\(\s+(.+?)\s+\)/)
			return unless m
			category = m[1]
			
			m = line.match(/^([^<]+?)\s*</)
			return unless m
			rtime = irctime_to_time(m[1] )

			url = 'http://polishtracker.net:81/downloadd.php/' + id.to_s + '/' + @config.pt[:rsskey] + '/' + name + '.torrent'
			
			o = { :id => id, :name => name, :category => category, :time => rtime, :url => url, :source => 'pt' }


			if m = line.match(/Rozmiar\s*:\s*\(\s+(.+?)\s+\)/)
				o[:size] = to_bytes( m[1] )
			end
			if m = line.match(/Gatunek\s*:\s*\(\s+(.+?)\s+\)/)
				o[:genre] = m[1]
			end
			@receivers.each { |receiver| receiver.consume(o) }
		end
	end
end

class ReleaseDb
	def initialize(receivers)
		@receivers = receivers
		reload
	end

	def key(object)
		[ 'name = ? AND source = ?', object[:name], object[:source] ]
	end

	def consume(object)
		k = key(object)
		if r = indb { ArchivedRelease.find(:first, :conditions => k) }
			yo = YAML::load(r.object)
			if yo[:url] != object[:url]
				puts "ARCHIVE-UPDATE: #{object[:name]}" 
				indb { MissingRelease.delete_all(k) }
				r.object = object.to_yaml
				indb { r.save!  }
			end
		elsif r = indb { Release.find(:first, :conditions => k) }
			yo = YAML::load(r.object)
			if yo[:url] != object[:url]
				puts "UPDATE: " + object[:name]
				indb { MissingRelease.delete_all(k) }
				r.object = object.to_yaml
				indb { r.save! }
			end
			@receivers.each do |r| r.consume(object) end unless indb { MissingRelease.find(:first, :conditions => [ 'name = ? and source = ?', object[:name], object[:source] ]) }
		else 
			if custom_filter(object) or object[:name].match(@filter)
				puts "AUTO-ARCHIVE: " + object[:name]
				indb { ArchivedRelease.from_object(object).save! }
			else
				@receivers.each do |r| r.consume(object) end
				puts 'N: ' + object[:name]
				indb { Release.from_object(object).save! }
			end
		end
	end

	def reload
		@filter = Filter.generate_super_regex
	end

	def custom_filter(object)
		object[:category] == 'XXX'
	end
end

class TVADL
	def initialize(downloader)
		@qualityorder = [ 'SKIP', '720P','720I','1080P','1080I','XVID', 'BZIUM' ]
		@downloader = downloader
		reload
	end

	def reload
		puts "Reloading TVADL"
		entries = indb { AdlEntry.find(:all, :conditions => [ 'type = ?', 'tv' ]) }
		@accepter = Regexp.new('^((' + entries.map { |entry|
			entry.data + "[._](S\\d+E\\d+(-?E\\d+)?|\\d+x\\d\\d)[._]"
		}.join(')|(') + '))')
		@empty = entries.empty?		
	end
	
	def tv_sig(relname)
		relname = relname.gsub(/[_ ]/,'.').gsub('..','.')
		if m = relname.match(/^(.+?)[._]S(\d+)E(\d+)E(\d+)/)
			[ m[1], m[2], m[3], m[4] ]
		elsif m = relname.match(/^(.+?)[._]S(\d+)E(\d+)/)
			[ m[1], m[2], m[3] ]
		elsif m = relname.match(/^(.+?)[._](\d+)x(\d+)/)
			[ m[1], m[2], m[3] ]
		else
			nil
		end
	end

	def get_downloaded_quality(sig)
		e = indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', sig.join('.') ]) }
		if e
			return e.quality || 'BZIUM'
		else
			return 'BZIUM'
		end
	end

	def tv_quality(relname)
		tokens = relname.upcase.split(/[._ ]/)
		@qualityorder.index(tokens.detect { |t| @qualityorder.index(t) }) || @qualityorder.size-1
	end
	
	# false - not downloaded/ok to download
	# true - already downloaded
	def downloaded?(relname)
		sig = tv_sig(relname)
		return false if sig.nil?
		tokens = relname.upcase.split(/[._ ]/)
		ourquality = tv_quality(relname)
		downloaded_ep = indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', sig.join('.') ]) }
		if downloaded_ep
			if tokens.index('PROPER') or tokens.index('REPACK')
				puts "Reason: PROPER or REPACK"
				return false # all propers must be downloaded
			elsif @qualityorder.index(get_downloaded_quality(sig)) > ourquality 
				puts "Reason: quality #{@qualityorder[ourquality]} is better then #{get_downloaded_quality(sig)}"
				return false # better version
			else
				return true
			end
		else
			return false
		end
	end


	def consume(object)
		if !@empty and object[:name].match(@accepter)
			dh = indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', object[:name]]) }
			if dh
				# skip
			elsif downloaded?(object[:name])
				puts "SKIP-TV-DOWNLOADED: " + object[:name]	
				DownloadHistory.create!(:name => object[:name], :quality => 'SKIP') 
			else
				puts "TVADL wants: #{object[:name]}"
				object[:addhistory] = [ { :name => tv_sig(object[:name]).join('.'), :quality => @qualityorder[tv_quality(object[:name])] } ]
				@downloader.download(object)
			end
		end
	end
end

class GenericADL
	def initialize(downloader)
		@downloader = downloader
		reload
	end
	def reload
		puts "Reloading GenericADL"
		entries = indb { AdlEntry.find(:all, :conditions => [ 'type = ?', 'generic' ]) }
		@accepter = Regexp.new('^((' + entries.map { |entry|
			entry.data
		}.join(')|(') + '))$')
		@empty = entries.empty?
	end
	def consume(object)
		if !@empty and object[:name].match(@accepter)
			puts "GenericADL wants: #{object[:name]}"
			@downloader.download(object)
		end
	end
end

class Downloader 
	def initialize(config)
		@config = config
#		@mutex = Mutex.new
	end
	def download(object, defer_on_error = true)
		result = nil
#		@mutex.synchronize { 
			if indb { defer_on_error and DeferredDownload.find(:first, :conditions => [ 'url = ?', object[:url] ]) } # deferdownloader sets defer_on_error=false
				puts "SKIP-ALREADY-DEFERRED: #{object[:name]}"
				result = false
			elsif indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', object[:name] ]) }
				puts "SKIP-ALREADY-DOWNLOADED: #{object[:name]}"
				result = true
			elsif indb { MissingRelease.find(:first, :conditions => [ 'name = ? and source = ?', object[:name], object[:source] ]) }
				puts "SKIP-404: #{object[:name]}"
				result = false
			elsif object[:addhistory] and object[:addhistory].detect { |ahe| indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', ahe[:name] ]) } }
				add_history_element = object[:addhistory].detect { |ahe| indb { DownloadHistory.find(:first, :conditions => [ 'name = ?', ahe[:name] ]) } }
				puts "SKIP-BETTER-ALTERNATIVE-ALREADY-DOWNLOADED: #{object[:name]} due to #{add_history_element[:name]}"
				result = true
			else
				puts "Download #{object[:name]}"
				begin
					# FIXME
#					url = object[:url].gsub(/polishtracker\.net/,'localhost')
					filename = File.join(@config.watchdir,object[:name]) + '.torrent'
					filenamed = filename + ".adldownload"
					puts "DOWNLOAD: #{object[:name]}/#{object[:url]} => #{filenamed}"
					open(url,'rb') do |srcf|
						open(filenamed, 'wb') do |dstf|
							dstf.write(srcf.read)
						end
					end
					File.rename(filenamed, filename)
					indb { DownloadHistory.create({ :name => object[:name], :additional => 'single'}) }
					if object[:addhistory]
						object[:addhistory].each do |ahe|
							puts "DOWNLOADER: Saving additional DH: " + ahe.inspect
							indb { DownloadHistory.create(ahe) }
						end
					end
					result = true
				rescue OpenURI::HTTPError => e
					if e.message.match(/^404/)
						puts "MISSING: #{object[:name]}/#{object[:url]} => #{e.message}"
						indb { MissingRelease.create(:source => object[:source], :name => object[:name], :log => (object[:url] + " => " + e.message) ) }
						result = false
					else
						puts "ERROR: #{object[:name]}/#{object[:url]} => #{e.message}"
						if defer_on_error
							indb { 
								DeferredDownload.from_object(object, e.inspect).save! unless DeferredDownload.find(:first, :conditions => [ 'url = ?', object[:url] ])
							}
							result = true
						else
							result = false
						end
					end
				rescue Exception => e
					puts "ERROR: #{object[:name]}/#{object[:url]} => #{e.message}"
					if defer_on_error
						indb { 
							DeferredDownload.from_object(object, e.inspect).save! unless DeferredDownload.find(:first, :conditions => [ 'url = ?', object[:url] ])
						}
						result = true
					else
						result = false
					end
				end
			end
#		}
		return result
	end
end


downloader = Downloader.new(config)



lw = LogWatcher.new(config, { 
	'pt' => PTModule.new(config, [ 
		ReleaseDb.new([
			TVADL.new(downloader),
			GenericADL.new(downloader)
		])
	])
}, DeferredDownloader.new(downloader))


lw.start
lw.join
