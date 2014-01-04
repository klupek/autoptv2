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

config = ConfigFile.new('config.yaml')

Thread::abort_on_exception=true
ActiveSupport::Deprecation.silenced = true
ActiveRecord::Base.logger = nil
ActiveRecord::Base.configurations = { 'default' => config.database }
ActiveRecord::Base.establish_connection('default')

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
		result = Regexp.new('^((' + Filter.find(:all).map { |x| x.name_regex }.join(')|(') + '))$')
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
		ActiveRecord::Base.connection.execute('CREATE TABLE deferred_downloads(id INTEGER PRIMARY KEY AUTOINCREMENT, object text not null)')
	end

	def self.from_object(object)
		DeferredDownload.create(:object => object.to_yaml)
	end
end

[ Release, ArchivedRelease, Filter, DownloadHistory, AdlEntry, MissingRelease, DeferredDownload ].each do |klass|
	begin
		klass.create_table
	rescue ActiveRecord::StatementInvalid 
	end
end

exit if ARGV[0] == 'generate-sql-tables-only'


class LogWatcher
	def initialize(config,modules)
		@config = config
		@modules = modules
		@files = config.sources.map { |logfile,src|
			f = File.open(logfile)
			f.seek(src[:offset] || 0, IO::SEEK_SET)
			{ :file => f, :module => src[:module], :filename => logfile }
		}
		@threads = []
		@consumer_thread = nil
		@queue = Queue.new
	end	

	def start
		@files.each { |f|
			@threads << Thread.new(f) { |fh|
				puts "Reading backlog for " + fh[:filename]
				lines = 0
				ActiveRecord::Base.connection.execute('begin transaction')
				begin
					loop do 
						line = fh[:file].readline
						@queue.push({ :line => line, :module => fh[:module], :filename => fh[:filename], :offset => fh[:file].pos}) unless line == ''
						lines += 1
					end
					rescue EOFError
				end	
				ActiveRecord::Base.connection.execute('commit')
				puts "Read #{lines} lines of backlog in " + fh[:filename]
				fh[:file].extend(File::Tail)
				fh[:file].interval = 10
				fh[:file].backward(10)
#				fh[:file].tail { |line|
#					@queue.push({ :line => line, :module => fh[:module], :filename => fh[:filename], :offset => fh[:file].pos})	
#				}
			}
		}
		@consumer_thread = Thread.new { 
			loop {
				task = @queue.pop
				if task == :die
					puts "LogWatcher is shutting down"
					break
				else
					@config.sources[task[:filename]][:offset] = task[:offset]
					@config.save # TODO: do it less frequent
					@modules[task[:module]].consume(task[:line])
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
		if r = ArchivedRelease.find(:first, :conditions => k) 
			yo = YAML::load(r.object)
			if yo[:url] != object[:url]
				MissingRelease.delete_all(k)
			end
			r.object = object.to_yaml
			r.save!
		elsif r = Release.find(:first, :conditions => k)
			yo = YAML::load(r.object)
			if yo[:url] != object[:url]
				puts "UPDATE: " + object[:name]
				MissingRelease.delete_all(k)
			end
			r.object = object.to_yaml
			r.save!
			@receivers.each do |r| r.consume(object) end
		else 
			if custom_filter(object) or object[:name].match(@filter)
				puts "AUTO-ARCHIVE: " + object[:name]
				ArchivedRelease.from_object(object).save!
			else
				@receivers.each do |r| r.consume(object) end
				puts 'N: ' + object[:name]
				Release.from_object(object).save!
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
		entries = AdlEntry.find(:all, :conditions => [ 'type = ?', 'tv' ])
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
		e = DownloadHistory.find(:first, :conditions => [ 'name = ?', sig.join('.') ])
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
		downloaded_ep = DownloadHistory.find(:first, :conditions => [ 'name = ?', sig.join('.') ])	
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
			dh = DownloadHistory.find(:first, :conditions => [ 'name = ?', object[:name]])
			if dh
				# skip
			elsif downloaded?(object[:name])
				puts "SKIP-TV-DOWNLOADED: " + object[:name]	
				DownloadHistory.create!(:name => object[:name], :quality => 'SKIP') 
			else
				puts "TVADL wants: #{object[:name]}"
				if @downloader.download(object)
					 dh = DownloadHistory.create!(:name => tv_sig(object[:name]).join("."), :quality => @qualityorder[tv_quality(object[:name])])
					 puts "TVADL saved #{dh.name}/#{dh.quality}"
				end
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
		entries = AdlEntry.find(:all, :conditions => [ 'type = ?', 'generic' ])
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
	end
	def download(object)
		if DownloadHistory.find(:first, :conditions => [ 'name = ?', object[:name] ])
			puts "SKIP-ALREADY-DOWNLOADED: #{object[:name]}"
			return true
		elsif MissingRelease.find(:first, :conditions => [ 'name = ?', object[:name] ])
			puts "SKIP-404: #{object[:name]}"
			return false
		else
			puts "Download #{object[:name]}"
			begin
				# FIXME
				url = object[:url].gsub(/polishtracker\.net/,'localhost')
				filename = File.join(@config.watchdir,object[:name]) + '.torrent'
				filenamed = filename + ".adldownload"
				puts "DOWNLOAD: #{object[:name]}/#{object[:url]} => #{filenamed}"
				open(url,'rb') do |srcf|
					open(filenamed, 'wb') do |dstf|
						dstf.write(srcf.read)
					end
				end
				File.rename(filenamed, filename)
				DownloadHistory.create({ :name => object[:name], :additional => 'single'})
			rescue OpenURI::HTTPError => e
				if e.message.match(/^404/)
					puts "MISSING: #{object[:name]}/#{object[:url]} => #{e.message}"
					MissingRelease.create(:source => object[:source], :name => object[:name], :log => (object[:url] + " => " + e.message) )
					return false
				else
					puts "ERROR: #{object[:name]}/#{object[:url]} => #{e.message}"
					DeferredDownload.from_object(object).save!
					return true
				end
			rescue Exception => e
				puts "ERROR: #{object[:name]}/#{object[:url]} => #{e.message}"
				DeferredDownload.from_object(object).save!
				return true
			end
			return true
		end
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
})
lw.start
lw.join
