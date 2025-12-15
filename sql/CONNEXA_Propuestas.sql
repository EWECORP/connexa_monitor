SELECT id, proposal_id, proposal_number, supplier_id, ext_code_supplier, name_supplier,
       site_id, ext_code_site, type_site, name,
       proposed_quantity, quantity_confirmed, total_amount, total_units, total_box, total_product,
       quantity_stock, occupation_pallet, occupation_pallet_proposed, approved
FROM supply_planning.view_spl_supply_purchase_proposal_supplier_site
WHERE ext_code_supplier = '5568'