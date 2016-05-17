#ifndef Util_IniFileConfigurationNew_INCLUDED
#define Util_IniFileConfigurationNew_INCLUDED


#include "Bmco/Util/Util.h"
#include "Bmco/Util/AbstractConfiguration.h"
#include <map>
#include <istream>


namespace Bmco {
namespace Util {


class Util_API IniFileConfigurationNew: public AbstractConfiguration
	/// This implementation of a Configuration reads properties
	/// from a legacy Windows initialization (.ini) file.
	///
	/// The file syntax is implemented as follows.
	///   - a line starting with a semicolon is treated as a comment and ignored
	///   - a line starting with a square bracket denotes a section key [<key>]
	///   - every other line denotes a property assignment in the form
	///     <value key> = <value>
	///
	/// The name of a property is composed of the section key and the value key,
	/// separated by a period (<section key>.<value key>).
	///
	/// Property names are not case sensitive. Leading and trailing whitespace is
	/// removed from both keys and values.
{
public:
	IniFileConfigurationNew();
		/// Creates an empty IniFileConfiguration.

	IniFileConfigurationNew(std::istream& istr);
		/// Creates an IniFileConfiguration and loads the configuration data
		/// from the given stream, which must be in initialization file format.
		
	IniFileConfigurationNew(const std::string& path);
		/// Creates an IniFileConfiguration and loads the configuration data
		/// from the given file, which must be in initialization file format.
		
	void load(std::istream& istr);
		/// Loads the configuration data from the given stream, which 
		/// must be in initialization file format.
		
	void load(const std::string& path);
		/// Loads the configuration data from the given file, which 
		/// must be in initialization file format.

	void save(std::ostream& ostr) const;
	/// Writes the configuration data to the given stream.
	///
	/// The data may not keep the original sequence.

	void save(const std::string& path) const;
	/// Writes the configuration data to the given file.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	~IniFileConfigurationNew();

private:
	void parseLine(std::istream& istr);

	struct ICompare
	{
		bool operator () (const std::string& s1, const std::string& s2) const;
	};
	typedef std::map<std::string, std::string, ICompare> IStringMap;
	
	IStringMap _map;
	std::string _sectionKey;
};


} } // namespace Bmco::Util


#endif // Util_IniFileConfigurationNew_INCLUDED
