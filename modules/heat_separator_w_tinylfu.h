#ifndef _HEAT_SEPARATOR_W_TINYLFU_H_
#define _HEAT_SEPARATOR_W_TINYLFU_H_

#include "separator.h"

#include <list>
#include <unordered_map>
#include <vector>
#include <optional>
#include <random>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <shared_mutex>


// Forked From: https://github.com/HONGYU-LEE/Window-TinyLFU-Cache
// ####################

namespace external_module
{

class BloomFilter
{
public:
	explicit BloomFilter(size_t capacity, double fp)
		: _bitsPerKey((getBitsPerKey(capacity, fp) < 32 ? 32 : _bitsPerKey) + 31)
		, _hashCount(getHashCount(capacity, _bitsPerKey))
		, _bits(_bitsPerKey, 0)
	{
		//如果哈希函数的数量超过上限，此时准确率就非常低
		if (_hashCount > MAX_HASH_COUNT)
		{
			_hashCount = MAX_HASH_COUNT;
		}
	}

	void put(uint32_t hash)
	{
		int delta = hash >> 17 | hash << 15;

		for (int i = 0; i < _hashCount; i++)
		{
			int bitPos = hash % _bitsPerKey;
			_bits[bitPos / 32] |= (1 << (bitPos % 32));
			hash += delta;
		}
	}

	bool contains(uint32_t hash) const 
	{
		//参考leveldb的做法，通过delta来打乱哈希值，模拟进行多次哈希函数
		int delta = hash >> 17 | hash << 15;	//右旋17位，打乱哈希值
		int bitPos = 0;
		for (int i = 0; i < _hashCount; i++) 
		{
			bitPos = hash % _bitsPerKey;
			if ((_bits[bitPos / 32] & (1 << (bitPos % 32))) == 0) 
			{
				return false;
			}
			hash += delta;
		}

		return true;
	}

	bool allow(uint32_t hash)
	{
		if (contains(hash) == false)
		{
			put(hash);
			return false;
		}
		return true;
	}

	void clear() 
	{
		for (auto& it : _bits) 
		{
			it = 0;
		}
	}

private:
	size_t _bitsPerKey;
	size_t _hashCount;
	std::vector<int> _bits;

	static const int MAX_HASH_COUNT = 30;

	int getBitsPerKey(int capacity, double fp) const
	{
		return std::ceil(-1 * capacity * std::log(fp) / std::pow(std::log(2), 2));
	}

	int getHashCount(int capacity, int bitsPerKey) const
	{
		return std::round(std::log(2) * bitsPerKey / double(capacity));
	}
};

class CountMinRow {
public:
	explicit CountMinRow(size_t countNum) 
		: _bits((countNum < 8 ? 8 : countNum) / 8, 0)
	{}

private:
	friend class CountMinSketch;
	std::vector<int> _bits;

	int get(int num) const {
		int countIndex = num / 8;
		int countOffset = num % 8 * 4;

		return (_bits[countIndex] >> countOffset) & 0x7;
	}


	//增加计数
	void increment(int num) {
		int countIndex = num / 8;
		int countOffset = num % 8 * 4;

		int value = (_bits[countIndex] >> countOffset) & 0x7;

		if (value < 15) {
			_bits[countIndex] += (1 << countOffset);
		}
	}

	void clear() {
		for (auto& it : _bits) {
			it = 0;
		}
	}

	//保鲜算法，使计数值减半
	void reset() {
		for (auto& it : _bits) {
			it = (it >> 1) & 0x77777777;
		}
	}
};

class CountMinSketch {
public:
	explicit CountMinSketch(size_t countNum)
		: _seed(4)
	{
		countNum = next2Power(countNum);
		if (countNum < 8) {
			countNum = 8;
		}
		_cmRows.resize(4, CountMinRow(countNum));
		_mask = countNum - 1;


		//利用当前时间作为generator的seed
		unsigned time = std::chrono::system_clock::now().time_since_epoch().count();
		std::mt19937 generator(time);
		for (int i = 0; i < COUNT_MIN_SKETCH_DEPTH; i++) {
			//随机生成一个32位seed
			generator.discard(generator.state_size);
			_seed[i] = generator();
		}
	}

	//选择几个cmRow最小的一个作为有效值返回
	int getCountMin(uint32_t hash) {
		int min = 16, value = 0;
		for (int i = 0; i < _cmRows.size(); i++) {
			value = _cmRows[i].get((hash ^ _seed[i]) & _mask);

			min = (value < min) ? value : min;
		}
		return min;
	}

	void Increment(uint32_t hash) {
		for (int i = 0; i < _cmRows.size(); i++) {
			_cmRows[i].increment((hash ^ _seed[i]) & _mask);
		}
	}

	void Reset() {
		for (auto& cmRow : _cmRows) {
			cmRow.reset();
		}
	}

	void Clear() {
		for (auto& cmRow : _cmRows) {
			cmRow.clear();
		}
	}

private:
	std::vector<CountMinRow> _cmRows;
	std::vector<uint32_t> _seed;	//用于打散哈希值
	uint32_t _mask;					//掩码用于取低n位

	static const int COUNT_MIN_SKETCH_DEPTH = 4;

	//用于计算下一个最接近x的二次幂
	int next2Power(size_t x) {
		x--;
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 8;
		x |= x >> 16;
		x |= x >> 32;
		x++;
		return x;
	}
};

enum SEGMENT_ZONE
{
	PROBATION,
	PROTECTION
};

template<typename T>
struct LRUNode
{
public:
	uint32_t _key;
  T _raw_key;
	T _value;
	uint32_t _conflict;	//在key出现冲突时的辅助hash值
	bool _flag;	//用于标记在缓存中的位置

	explicit LRUNode(uint32_t key = -1, const T& raw_key = T(), const T& value = T(), uint32_t conflict = 0, bool flag = PROBATION)
		: _key(key)
    , _raw_key(raw_key)
		, _value(value)
		, _conflict(conflict)
		, _flag(flag)
	{}

};

template<typename T>
class LRUCache
{
public:
	typedef LRUNode<T> LRUNode_t;

	explicit LRUCache(size_t capacity)
		: _capacity(capacity)
		, _hashmap(capacity)
	{}

	std::pair<LRUNode_t, bool> get(const LRUNode_t& node)
	{
		auto res = _hashmap.find(node._key);
		if (res == _hashmap.end())
		{
			return std::make_pair(LRUNode_t(), false);
		}

		//获取数据的位置
		typename std::list<LRUNode_t>::iterator pos = res->second;
		LRUNode_t curNode = *pos;

		//将数据移动到队首
		_lrulist.erase(pos);
		_lrulist.push_front(curNode);

		res->second = _lrulist.begin();

		return std::make_pair(curNode, true);
	}

	std::pair<LRUNode_t, bool> put(const LRUNode_t& node)
	{
		bool flag = false; //是否置换数据
		LRUNode_t delNode;

		//数据已满，淘汰末尾元素
		if (_lrulist.size() == _capacity)
		{
			delNode = _lrulist.back();
			_lrulist.pop_back();
			_hashmap.erase(delNode._key);

			flag = true;
		}
		//插入数据
		_lrulist.push_front(node);
		_hashmap.insert(make_pair (node._key, _lrulist.begin()));
		return std::make_pair(delNode, flag);
	}

	size_t capacity() const
	{
		return _capacity;
	}

	size_t size() const 
	{
		return _lrulist.size();
	}

private:
	size_t _capacity;
	//利用哈希表来存储数据以及迭代器，来实现o(1)的get和put
	std::unordered_map<int, typename std::list<LRUNode_t>::iterator> _hashmap;
	//利用双向链表来保存缓存使用情况，并保证o(1)的插入删除
	std::list<LRUNode_t> _lrulist;
};

template<typename T>
class SegmentLRUCache
{
public:
	typedef LRUNode<T> LRUNode_t;

	explicit SegmentLRUCache(size_t probationCapacity, size_t protectionCapacity)
		: _probationCapacity(probationCapacity)
		, _protectionCapacity(protectionCapacity)
		, _hashmap(probationCapacity +  protectionCapacity)
	{}

	std::pair<LRUNode_t, bool> get(LRUNode_t& node)
	{
		//找不到则返回空
		auto res = _hashmap.find(node._key);
		if (res == _hashmap.end())
		{
			return std::make_pair(LRUNode_t(), false);
		}

		//获取数据的位置
		typename std::list<LRUNode_t>::iterator pos = res->second;

		//如果查找的值在PROTECTION区中，则直接移动到首部
		if (node._flag == PROTECTION)
		{
			_protectionList.erase(pos);

			_protectionList.push_front(node);
			res->second = _protectionList.begin();
			
			return std::make_pair(node, true);
		}

		//如果是PROBATION区的数据，如果PROTECTION还有空位，则将其移过去
		if (_protectionList.size() < _probationCapacity)
		{
			node._flag = PROTECTION;
			_probationList.erase(pos);

			_protectionList.push_front(node);
			res->second = _protectionList.begin();

			return std::make_pair(node, true);
		}

		//如果PROTECTION没有空位，此时就将其最后一个与PROBATION当前元素进行交换位置
		LRUNode_t backNode = _protectionList.back();

		std::swap(backNode._flag, node._flag);

		_probationList.erase(_hashmap[node._key]);
		_protectionList.erase(_hashmap[backNode._key]);

		_probationList.push_front(backNode);
		_protectionList.push_front(node);

		_hashmap[backNode._key] = _probationList.begin();
		_hashmap[node._key] = _protectionList.begin();


		return std::make_pair(node, true);
	}

	std::pair<LRUNode_t, bool> put(LRUNode_t& newNode)
	{
		//新节点放入PROBATION段中
		newNode._flag = PROBATION;
		LRUNode_t delNode;

		//如果还有剩余空间就直接插入
		if (_probationList.size() < _probationCapacity || size() < _probationCapacity + _protectionCapacity)
		{
			_probationList.push_front(newNode);
			_hashmap.insert(make_pair(newNode._key, _probationList.begin()));

			return std::make_pair(delNode, false);
		}

		//如果没有剩余空间，就需要淘汰掉末尾元素，然后再将元素插入首部
		delNode = _probationList.back();

		_hashmap.erase(delNode._key);
		_probationList.pop_back();

		_probationList.push_front(newNode);
		_hashmap.insert(make_pair(newNode._key, _probationList.begin()));

		return std::make_pair(delNode, true);
	}

	std::pair<LRUNode_t, bool> victim()
	{
		//如果还有剩余的空间，就不需要淘汰
		if (_probationCapacity + _protectionCapacity > size())
		{
			return std::make_pair(LRUNode_t(), false);
		}

		//否则淘汰_probationList的最后一个元素
		return std::make_pair(_probationList.back(), true);
	}

	int size() const
	{
		return _protectionList.size() + _probationList.size();
	}

	std::list<LRUNode_t> get_protection_list()
	{
		return this->_protectionList;
	}
	std::list<LRUNode_t> get_probation_list()
	{
		return this->_probationList;
	}

private:
	size_t _probationCapacity;
	size_t _protectionCapacity;

	std::unordered_map<int, typename std::list<LRUNode_t>::iterator> _hashmap;

	std::list<LRUNode_t> _probationList;
	std::list<LRUNode_t> _protectionList;
};

enum CACHE_ZONE
{
	SEGMENT_LRU,
	WINDOWS_LRU
};

template<typename V>
class WindowsTinyLFU
{
public:
	typedef LRUCache<V> LRUCache_t;
	typedef LRUNode<V> LRUNode_t;
	typedef SegmentLRUCache<V> SegmentLRUCache_t;

	explicit WindowsTinyLFU(size_t capacity)
		: _wlru(std::ceil(capacity * 0.01))
		, _slru(std::ceil(0.2 * (capacity * (1 - 0.01))), std::ceil((1 - 0.2) * (capacity * (1 - 0.01))))
		, _bloomFilter(capacity, BLOOM_FALSE_POSITIVE_RATE / 100)
		, _cmSketch(capacity)
		, _dataMap(capacity)
		, _totalVisit(0)
		, _threshold(100)
	{}

	std::pair<V, bool> Get(const std::string& key)
	{
		uint32_t keyHash = Hash(key.c_str(), key.size(), KEY_HASH_SEED);
		uint32_t conflictHash = Hash(key.c_str(), key.size(), CONFLICT_HASH_SEED);

		std::shared_lock<std::shared_mutex> rLock(_rwMutex);
		return get(keyHash, conflictHash);
	}

	std::pair<V, bool> Del(const std::string& key)
	{
		uint32_t keyHash = Hash(key.c_str(), key.size(), KEY_HASH_SEED);
		uint32_t conflictHash = Hash(key.c_str(), key.size(), CONFLICT_HASH_SEED);

		std::unique_lock<std::shared_mutex> wLock(_rwMutex);	
		return del(keyHash, conflictHash);
	}

	bool Put(const std::string& key, const V& value)
	{
		uint32_t keyHash = Hash(key.c_str(), key.size(), KEY_HASH_SEED);
		uint32_t conflictHash = Hash(key.c_str(), key.size(), CONFLICT_HASH_SEED);

		std::unique_lock<std::shared_mutex> wLock(_rwMutex);
		return put(keyHash, key, value, conflictHash);
	}

	static unsigned int Hash(const void* key, int len, int seed)
	{
		const unsigned int m = 0x5bd1e995;
		const int r = 24;
		unsigned int h = seed ^ len;

		const unsigned char* data = (const unsigned char*)key;
		while (len >= 4)
		{
			unsigned int k = *(unsigned int*)data;
			k *= m;
			k ^= k >> r;
			k *= m;
			h *= m;
			h ^= k;
			data += 4;
			len -= 4;
		}

		switch (len)
		{
		case 3: h ^= data[2] << 16;
		case 2: h ^= data[1] << 8;
		case 1: h ^= data[0];
			h *= m;
		};

		h ^= h >> 13;
		h *= m;
		h ^= h >> 15;
		return h;
	}

  // 拓展外部实现，获取所有热 Keys
  bool GetHotKeys(std::vector<std::string>& hot_keys)
  {
    std::unique_lock<std::shared_mutex> wLock(_rwMutex);

    auto protected_list = _slru.get_protection_list();
		auto probation_list = _slru.get_probation_list();
    for (auto& node : protected_list)
      hot_keys.push_back(node._raw_key);
		for (auto& node : probation_list)
			hot_keys.push_back(node._raw_key);

    return true;
  }

private:
	LRUCache_t _wlru;
	SegmentLRUCache_t _slru;
	BloomFilter _bloomFilter;
	CountMinSketch _cmSketch;
	std::shared_mutex _rwMutex;
	std::unordered_map<int, LRUNode_t> _dataMap;	//用于记录数据所在的区域
	size_t _totalVisit;
	size_t _threshold;	//保鲜机制

	static const int BLOOM_FALSE_POSITIVE_RATE = 1;		
	static const int KEY_HASH_SEED = 0xbc9f1d34;
	static const int CONFLICT_HASH_SEED = 0x9ae16a3b;


	std::pair<V, bool> get(uint32_t keyHash, uint32_t conflictHash)
	{
		//判断访问次数，如果访问次数达到限制，则触发保鲜机制
		++_totalVisit;
		if (_totalVisit >= _threshold)
		{
			_cmSketch.Reset();
			_bloomFilter.clear();
			_totalVisit = 0;
		}

		//在cmSketch对访问计数
		_cmSketch.Increment(keyHash);

		//首先查找map，如果map中没有找到则说明不存在
		auto res = _dataMap.find(keyHash);
		if (res == _dataMap.end())
		{
			return std::make_pair(V(), false);
		}

		LRUNode_t node = res->second;
		V value;

		//校验conflictHash是否一致，防止keyHash冲突查到错误数据
		if (node._conflict != conflictHash)
		{
			return std::make_pair (V(), false);
		}

		//判断数据位于slru还是wlru，进入对应的缓存中查询
		if (node._flag == WINDOWS_LRU)
		{
			_wlru.get(node);
		}
		else
		{
			_slru.get(node);
		}
		return std::make_pair(node._value, true);
	}

	std::pair<V, bool> del(uint32_t keyHash, uint32_t conflictHash)
	{
		auto res = _dataMap.find(keyHash);
		if (res == _dataMap.end())
		{
			return std::make_pair(V(), false);
		}

		LRUNode_t node = res->second;

		//再次验证conflictHash是否相同，防止由于keyHash冲突导致的误删除
		if (node._conflict != conflictHash)
		{
			return std::make_pair(V(), false);
		}
		
		_dataMap.erase(keyHash);
		return std::make_pair(node._value, true);
	}

	bool put(uint32_t keyHash, const V& key, const V& value, uint32_t conflictHash)
	{

		LRUNode_t newNode(keyHash, key, value, conflictHash, WINDOWS_LRU);
		//将数据放入wlru，如果wlru没满，则直接返回
		std::pair<LRUNode_t, bool> res = _wlru.put(newNode);
		if (res.second == false)
		{
			_dataMap[keyHash] = newNode;
			return true;
		}

		//如果此时发生了淘汰，将淘汰节点删去
		if (_dataMap[res.first._key]._flag == WINDOWS_LRU)
		{
			_dataMap.erase(res.first._key);
		}
		

		//如果slru没满，则插入到slru中。
		newNode._flag = SEGMENT_LRU;

		std::pair<LRUNode_t, bool> victimRes = _slru.victim();

		if (victimRes.second == false)
		{
			_dataMap[keyHash] = newNode;
			_slru.put(newNode);
			return true;
		}

		//如果该值没有在布隆过滤器中出现过，其就不可能比victimNode高频，则插入布隆过滤器后返回
		if (!_bloomFilter.allow(keyHash))
		{
			return false;
		}

		//如果其在布隆过滤器出现过，此时就对比其和victim在cmSketch中的计数，保留大的那一个
		int victimCount = _cmSketch.getCountMin(victimRes.first._key);
		int newNodeCount = _cmSketch.getCountMin(newNode._key);

		//如果victim大于当前节点，则没有插入的必要
		if (newNodeCount < victimCount)
		{
			return false;
		}
		_dataMap[keyHash] = newNode;
		_slru.put(newNode);

		//如果满了，此时发生了淘汰，将淘汰节点删去
		if (_dataMap[res.first._key]._flag == SEGMENT_LRU)
		{
			_dataMap.erase(victimRes.first._key);
		}

		return true;
	}
};

}

// ####################

namespace module
{

/** 实质思路和 Count-Min Sketch + LRU 时间窗口思路很像 */
class HeatSepratorWTinyLFU : public HeatSeparator
{
private:
  // 直接使用外部实现
  external_module::WindowsTinyLFU<std::string> w_tinyflu;
  size_t capacity;

  Status UpdateAccessKey(const std::string& key);

public:
  HeatSepratorWTinyLFU(const size_t capacity);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif