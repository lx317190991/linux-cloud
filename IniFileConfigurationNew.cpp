#include "IniFileConfigurationNew.h"
#include "Bmco/Exception.h"
#include "Bmco/String.h"
#include "Bmco/Path.h"
#include "Bmco/FileStream.h"
#include "Bmco/LineEndingConverter.h"
#include "Bmco/Ascii.h"
#include <set>


using Bmco::icompare;
using Bmco::trim;
using Bmco::Path;


namespace Bmco {
namespace Util {


IniFileConfigurationNew::IniFileConfigurationNew()
{
}


IniFileConfigurationNew::IniFileConfigurationNew(std::istream& istr)
{
	load(istr);
}

	
IniFileConfigurationNew::IniFileConfigurationNew(const std::string& path)
{
	load(path);
}


IniFileConfigurationNew::~IniFileConfigurationNew()
{
}


void IniFileConfigurationNew::load(std::istream& istr)
{
	_map.clear();
	_sectionKey.clear();
	while (!istr.eof())
	{
		parseLine(istr);
	}
}
	

void IniFileConfigurationNew::load(const std::string& path)
{
	Bmco::FileInputStream istr(path);
	if (istr.good())
		load(istr);
	else
		throw Bmco::OpenFileException(path);
}

void IniFileConfigurationNew::save(std::ostream& ostr) const
{
	IStringMap::const_iterator itr = _map.begin();
	std::string section;
	while (itr!=_map.end())
	{
		size_t prefixPos = itr->first.find('.');
		std::string currentSection = itr->first.substr(0, prefixPos);
		if (currentSection != section) {
			ostr << "[" << currentSection << "]" << std::endl;
			section = currentSection;
		}
		if (prefixPos != std::string::npos) { // it's a prrotection code. "parseLine" will skip empty section.
			ostr << itr->first.substr(prefixPos+1) << " = " << itr->second << std::endl;
		}
		itr++;
	}
}


void IniFileConfigurationNew::save(const std::string& path) const
{
	Bmco::FileOutputStream ostr(path);
	if (ostr.good())
	{
		Bmco::OutputLineEndingConverter lec(ostr);
		save(lec);
		lec.flush();
		ostr.flush();
		if (!ostr.good()) throw Bmco::WriteFileException(path);
	}
	else throw Bmco::CreateFileException(path);
}


bool IniFileConfigurationNew::getRaw(const std::string& key, std::string& value) const
{
	IStringMap::const_iterator it = _map.find(key);
	if (it != _map.end())
	{
		value = it->second;
		return true;
	}
	else return false;
}


void IniFileConfigurationNew::setRaw(const std::string& key, const std::string& value)
{
	_map[key] = value;
}


void IniFileConfigurationNew::enumerate(const std::string& key, Keys& range) const
{
	std::set<std::string> keys;
	std::string prefix = key;
	if (!prefix.empty()) prefix += '.';
	std::string::size_type psize = prefix.size();
	for (IStringMap::const_iterator it = _map.begin(); it != _map.end(); ++it)
	{
		if (icompare(it->first, psize, prefix) == 0)
		{
			std::string subKey;
			std::string::size_type end = it->first.find('.', psize);
			if (end == std::string::npos)
				subKey = it->first.substr(psize);
			else
				subKey = it->first.substr(psize, end - psize);
			if (keys.find(subKey) == keys.end())
			{
				range.push_back(subKey);
				keys.insert(subKey);
			}
		}
	}
}


void IniFileConfigurationNew::removeRaw(const std::string& key)
{
	std::string prefix = key;
	if (!prefix.empty()) prefix += '.';
	std::string::size_type psize = prefix.size();
	IStringMap::iterator it = _map.begin();
	IStringMap::iterator itCur;
	while (it != _map.end())
	{
		itCur = it++;
		if ((icompare(itCur->first, key) == 0) || (icompare(itCur->first, psize, prefix) == 0))
		{
			_map.erase(itCur);
		}
	}
}


bool IniFileConfigurationNew::ICompare::operator () (const std::string& s1, const std::string& s2) const
{
	return icompare(s1, s2) < 0;
}


void IniFileConfigurationNew::parseLine(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof(); 

	int c = istr.get();
	while (c != eof && Bmco::Ascii::isSpace(c)) c = istr.get();
	if (c != eof)
	{
		if (c == ';')
		{
			while (c != eof && c != '\n') c = istr.get();
		}
		else if (c == '[')
		{
			std::string key;
			c = istr.get();
			while (c != eof && c != ']' && c != '\n') { key += (char) c; c = istr.get(); }
			_sectionKey = trim(key);
		}
		else
		{
			std::string key;
			while (c != eof && c != '=' && c != '\n') { key += (char) c; c = istr.get(); }
			std::string value;
			if (c == '=')
			{
				c = istr.get();
				while (c != eof && c != '\n') { value += (char) c; c = istr.get(); }
			}
			std::string fullKey = _sectionKey;
			if (!fullKey.empty()) fullKey += '.';
			fullKey.append(trim(key));
			_map[fullKey] = trim(value);
		}
	}
}


} } // namespace Bmco::Util
