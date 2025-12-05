"""
NMDC 病原菌数据爬取脚本
API: https://nmdc.cn/gcpathogenapi/species/getallmetatable
"""

import requests
import pandas as pd
import json
from typing import List, Dict, Optional
import time
from datetime import datetime

class NMDCPathogenScraper:
	def __init__(self):
		self.api_url = "https://nmdc.cn/gcpathogenapi/species/getallmetatable"
		self.headers = {
			'Accept': 'application/json, text/plain, */*',
			'Accept-Encoding': 'gzip, deflate, br, zstd',
			'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
			'Content-Type': 'application/json',
			'Origin': 'https://nmdc.cn',
			# 重要
			'Referer': 'https://nmdc.cn/gcpathogen/pathogens?type={}',
			'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
			'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
			'sec-ch-ua-mobile': '?0',
			'sec-ch-ua-platform': '"macOS"',
			'sec-fetch-dest': 'empty',
			'sec-fetch-mode': 'cors',
			'sec-fetch-site': 'same-origin'
		}
	
	def create_payload(self, page: int = 1, page_size: int = 50, pathogen_type: str = "bacteria") -> dict:
		"""创建请求载荷"""
		payload = {
			"otherType": "taxa",
			"page": page,
			"high": "",
			"seaechStrainName": "",
			"searchAssembly": "",
			"searchAssemblyLevel": "",
			"searchAssemblyMethod": "",
			"searchContigsEnd": "",
			"searchContigsStart": "",
			"searchCountry": "",
			"searchDisease": "",
			"searchHost": "",
			"searchGene": "",
			"searchIsoSource": "",
			"searchLevel": "",
			"searchMLST": "",
			"searchPathogenName": "",
			"searchSequencing": "",
			"searchSubmitter": "",
			"searchTaxonid": "",
			"searchAroName": "",
			"searchVFName": "",
			"size": page_size,
			"type": pathogen_type,
			"ispublic": "",
			"searchDate": "",
			"pathogen": "",
			"host": "",
			"level": "",
			"disease": "",
			"country": "",
			"date": "",
			"aroName": "",
			"vf_name": "",
			"isoSource": "",
			"searchGenome": "",
			"pathogenName": "",
			"gramStain": "",
			"genomeType": "",
			"searchGenomeType": "",
			"searchGramStain": ""
		}
		return payload
	
	def get_page_data(self, page: int = 1, page_size: int = 50, pathogen_type: str = "bacteria") -> Optional[Dict]:
		"""
		获取指定页的数据
		
		Args:
			page: 页码（从1开始）
			page_size: 每页数量（建议50-100）
			pathogen_type: 病原体类型 (bacteria, virus, fungi等)
		
		Returns:
			响应数据字典，失败返回None
		"""
		payload = self.create_payload(page, page_size, pathogen_type)
		self.headers['Referer'] = self.headers['Referer'].format(pathogen_type)
		try:
			response = requests.post(
				self.api_url,
				headers=self.headers,
				json=payload,
				timeout=30
			)
			response.raise_for_status()
			return response.json()
			
		except requests.exceptions.RequestException as e:
			print(f"❌ 请求失败: {e}")
			return None
	
	def extract_records(self, response_data: Dict) -> tuple[List[Dict], int, int]:
		"""
		从响应中提取记录和分页信息
		
		Returns:
			(记录列表, 总记录数, 总页数)
		"""
		if not response_data:
			return [], 0, 0
		
		# 提取记录
		records = []
		if 'data' in response_data:
			if isinstance(response_data['data'], dict):
				#records = response_data['data'].get('records', [])
				records = response_data['data'].get('list', [])
			elif isinstance(response_data['data'], list):
				records = response_data['data']
		elif 'records' in response_data:
			records = response_data['records']
		elif isinstance(response_data, list):
			records = response_data
		
		# 提取总数信息
		total = 0
		total_pages = 0
		
		if 'data' in response_data and isinstance(response_data['data'], dict):
			total = response_data['data'].get('totalRow', 0)
			total_pages = response_data['data'].get('totalPage', 0)
		else:
			total = response_data.get('total', 0)
			total_pages = response_data.get('pages', 0)
		
		return records, total, total_pages
	
	def scrape_all_data(self, 
					pathogen_type: str = "bacteria", 
					page_size: int = 50,
					max_pages: Optional[int] = None,
					start_page: int = 1) -> List[Dict]:
		"""
		爬取所有数据
		
		Args:
			pathogen_type: 病原体类型 (bacteria, virus, fungi, etc.)
			page_size: 每页数量（建议50-100，太大可能超时）
			max_pages: 最大爬取页数限制（None表示爬取全部）
			start_page: 起始页码（用于断点续爬）
		
		Returns:
			所有记录的列表
		"""
		print(f"\n{'='*70}")
		print(f"开始爬取 {pathogen_type.upper()} 类型的病原体数据")
		print(f"每页数量: {page_size} | 起始页: {start_page}")
		print(f"{'='*70}\n")
		
		all_records = []
		page = start_page
		
		# 获取第一页，确定总页数
		print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在获取第 {page} 页...")
		first_response = self.get_page_data(page=page, page_size=page_size, pathogen_type=pathogen_type)
		
		if not first_response:
			print("❌ 无法获取数据，请检查网络或API是否正常")
			return []
		
		# 保存第一页原始响应用于调试
		date_tage = datetime.now().strftime('%Y%m%d')
		sample_json = f"response_sample_{pathogen_type}_{date_tage}.json"
		with open(sample_json, 'w', encoding='utf-8') as f:
			json.dump(first_response, f, ensure_ascii=False, indent=2)
		print(f"✓ 第一页原始响应已保存到 {sample_json}")
  
		# 提取数据总量和页数
		records, total, total_pages = self.extract_records(first_response)
		
		if not records:
			print("❌ 未找到数据记录")
			return []
		
		all_records.extend(records)
		
		print(f"✓ 第 {page} 页: {len(records)} 条记录")
		print(f"\n数据统计:")
		print(f"  - 总记录数: {total}")
		print(f"  - 总页数: {total_pages}")
		print(f"  - 每页数量: {page_size}")
		
		if max_pages:
			total_pages = min(total_pages, max_pages)
			print(f"  - 限制爬取: {max_pages} 页")
		
		# 爬取剩余页面
		if total_pages > page:
			print(f"\n开始爬取剩余 {total_pages - page} 页...\n")
			
			for page in range(start_page + 1, total_pages + 1):
				print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在获取第 {page}/{total_pages} 页...", end=' ')
				
				try:
					response = self.get_page_data(page=page, page_size=page_size, pathogen_type=pathogen_type)
					
					if not response:
						print(f"❌ 获取失败")
						continue
					
					records, _, _ = self.extract_records(response)
					
					if not records:
						print(f"⚠ 无数据，跳过")
						continue
					
					all_records.extend(records)
					print(f"✓ {len(records)} 条记录 (累计: {len(all_records)})")
					
					# 礼貌延迟，避免请求过快
					time.sleep(0.5)
					
				except Exception as e:
					print(f"❌ 异常: {e}")
					continue
		
		print(f"\n{'='*70}")
		print(f"✓ 爬取完成！共获取 {len(all_records)} 条记录")
		print(f"{'='*70}\n")
		
		return all_records
	
	def save_to_files(self, data: List[Dict], filename: str = "nmdc_pathogens"):
		"""保存数据到多种格式"""
		if not data:
			print("❌ 没有数据可保存")
			return
		
		print(f"正在保存数据到文件...\n")
		
		# 转换为DataFrame
		df = pd.DataFrame(data)
		
		# 保存为TSV（Excel兼容）
		tsv_file = f"{filename}.tsv"
		df.to_csv(tsv_file, index=False, encoding='utf-8-sig',sep="\t")
		print(f"✓ TSV文件: {tsv_file}")
		
		# 保存为Excel
		try:
			excel_file = f"{filename}.xlsx"
			df.to_excel(excel_file, index=False, engine='openpyxl')
			print(f"✓ Excel文件: {excel_file}")
		except Exception as e:
			print(f"⚠ Excel保存失败: {e}")
		
		# 保存为JSON
		json_file = f"{filename}.json"
		with open(json_file, 'w', encoding='utf-8') as f:
			json.dump(data, f, ensure_ascii=False, indent=2)
		print(f"✓ JSON文件: {json_file}")
		
		# 数据统计
		print(f"\n{'='*70}")
		print(f"数据统计:")
		print(f"  - 总记录数: {len(data)}")
		print(f"  - 字段数: {len(df.columns)}")
		print(f"  - 文件大小: TSV={self._get_file_size(tsv_file)}, JSON={self._get_file_size(json_file)}")
		print(f"{'='*70}\n")
		
		# 显示字段列表
		print("数据字段列表:")
		for i, col in enumerate(df.columns, 1):
			print(f"  {i:2d}. {col}")
		
		# 显示数据预览
		print(f"\n数据预览（前3条）:")
		print(df.head(3).to_string())
		print(f"\n...")
	
	def _get_file_size(self, filepath: str) -> str:
		"""获取文件大小（人类可读格式）"""
		try:
			import os
			size = os.path.getsize(filepath)
			for unit in ['B', 'KB', 'MB', 'GB']:
				if size < 1024:
					return f"{size:.2f} {unit}"
				size /= 1024
			return f"{size:.2f} TB"
		except:
			return "未知"


def main():
	"""主函数 - 示例用法"""
	scraper = NMDCPathogenScraper()
	
	# 方式1: 爬取所有细菌数据（推荐每页50-100条）每页10条
	print("=" * 70)
	print("开始爬取 BACTERIA (细菌) 数据")
	print("=" * 70)
	for pathogen_type in ["bacteria","fungi","virus",'parasite']:
		bacteria_data = scraper.scrape_all_data(
									pathogen_type=pathogen_type,
									page_size=10,  # 每页100条，速度更快
									max_pages=100   # None=爬取全部，可以设置数字限制页数，如 max_pages=10
									)
		
		if bacteria_data:
			date_tag = datetime.now().strftime('%Y%m%d')
			scraper.save_to_files(bacteria_data, filename=f"nmdc_{pathogen_type}_{date_tag}")
	
	# 方式2: 爬取其他类型数据（取消注释即可使用）
	"""
	# 爬取病毒数据
	print("\n" + "=" * 70)
	print("开始爬取 VIRUS (病毒) 数据")
	print("=" * 70)
	virus_data = scraper.scrape_all_data(
		pathogen_type="virus",
		page_size=100
	)
	if virus_data:
		scraper.save_to_files(virus_data, filename="nmdc_virus")
	
	# 爬取真菌数据
	print("\n" + "=" * 70)
	print("开始爬取 FUNGI (真菌) 数据")
	print("=" * 70)
	fungi_data = scraper.scrape_all_data(
		pathogen_type="fungi",
		page_size=100
	)
	if fungi_data:
		scraper.save_to_files(fungi_data, filename="nmdc_fungi")
	"""
	
	print("\n" + "=" * 70)
	print("✓ 所有任务完成！")
	print("=" * 70)


if __name__ == "__main__":
	main()
