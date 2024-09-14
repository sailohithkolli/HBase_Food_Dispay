#Your Jruby script code goes here
require 'time'

import 'org.apache.hadoop.hbase.client.HTable'
import 'org.apache.hadoop.hbase.client.Put'
import 'javax.xml.stream.XMLStreamConstants'
import 'org.apache.hadoop.hbase.client.Scan'
import 'org.apache.hadoop.hbase.util.Bytes'

def jbytes( *args )
    return args.map { |arg| arg.to_s.to_java_bytes }
end

foods_table = HTable.new( @hbase.configuration, 'foods' )

scanner = foods_table.getScanner( Scan.new )

count = 0

content = STDIN.read

food_blocks = %r\<Food_Display_Row>(.+?)</Food_Display_Row>\

print "Matches found: " 
print content.scan(food_blocks).size

pattern_foodkey= %r\<Display_Name>(.+?)</Display_Name>\
pattern_general = %r\<(.+?)>(.+?)</\

content.scan(food_blocks) do |food_data|

	
	keys = pattern_foodkey.match(food_data[0])[1]
	puts keys
	p = Put.new( *jbytes( keys ) ) 
	food_data[0].scan(pattern_general) do |captures|
		p.add( *jbytes( "fact", captures[0], captures[1] ) )
	end
	p.setWriteToWAL( false )
	foods_table.put( p ) 

end


#Do not remove the exit call below
exit
