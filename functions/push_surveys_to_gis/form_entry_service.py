class FormEntryService:
    def convert_form_entry_to_gis_object(self, form_entry_collection):
        form_entry = form_entry_collection["features"][0]
        form_entry_properties = form_entry["properties"]

        return {
            "geometry": {
                "x": form_entry["geometry"]["coordinates"][0],
                "y": form_entry["geometry"]["coordinates"][1],
            },
            "attributes": {
                "sleutel": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["FCA_SLEUTEL"],
                "postcode": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["DSS"][:6],
                "huisnummer": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["DSS"][6:],
                "bon_naam": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["BONNAAM"],
                "fca_project": form_entry_properties["Entry"]["AnswersJson"][
                    "SCHOUW_GEGEVENS_PAGE"
                ]["FCA_PROJECT"],
                "rd_x_coordinaat": form_entry["geometry"]["coordinates"][0],
                "rd_y_coordinaat": form_entry["geometry"]["coordinates"][1],
                "foto_bop": form_entry_properties["Entry"]["AnswersJson"][
                    "AFRONDEN_PAGE"
                ]["FCA_FOTO_GEVEL"],
            },
        }
